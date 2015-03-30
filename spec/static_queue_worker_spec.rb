# encoding: utf-8

require 'politics'

class UninitializedWorker
  include Politics::StaticQueueWorker
  def initialize(memcache_client)
    @memcache_client = memcache_client
    log.level = Logger::FATAL
  end

  def start
    process_bucket do |bucket|
      sleep 0.1
    end
  end

  def client_for(servers)
    @memcache_client
  end

  def local_ip
    IPAddr.new("127.0.0.1")
  end

  def at_exit
  end
end

class Worker < UninitializedWorker
  def initialize(memcache_client)
    super
    register_worker 'worker', 10, :iteration_length => 10
  end
end

describe UninitializedWorker do
  let(:memcache_client) { double('memcache', :set => nil, :get => nil) }
  let(:worker) { UninitializedWorker.new memcache_client}

  describe "when initializing" do
    it "should register the removal of the leadership as exit handler" do
      expect(worker).to receive(:at_exit).ordered {|&h| h}
      handler = worker.register_worker('worker', 10, :iteration_length => 10)

      expect(worker).to receive(:internal_cleanup).ordered
      expect(worker).to receive(:cleanup).ordered
      handler.call
    end

    it "should have a druby url" do
      worker.register_worker('worker', 10, :iteration_length => 10)
      expect(worker.uri).to match(%r|druby://.*:[0-9]+|)
    end

    it "should not have a hostname" do
      expect(worker.send(:hostname)).to be_nil
    end

    it "should not have a bucket_request_context" do
      expect(worker.send(:bucket_request_context)).to be_nil
    end

    context "when it has a hostname" do
      before do
        allow(worker).to receive(:hostname).and_return '127.0.0.1'
      end

      it "should use it" do
        worker.register_worker('worker', 10, :iteration_length => 10)
        expect(worker.uri).to match(%r|druby://127.0.0.1:[0-9]+|)
      end
    end

    context "when it does not have a hostname" do
      before do
        allow(worker).to receive(:hostname).and_return nil
      end

      it "should use the systems hostname" do
        worker.register_worker('worker', 10, :iteration_length => 10)
        expect(worker.uri).to match(%r|druby://#{`hostname -f`.chomp}:[0-9]+|)
      end
    end
  end
end

describe Worker do
  let(:memcache_client) { double('memcache', :set => nil, :get => nil) }
  let(:worker) { Worker.new memcache_client }

  it "should provide 'until_next_iteration' even if nominate was not completed" do
    worker.until_next_iteration
  end

  it "should return time to next iteration even if nominate was not completed" do
    expect(worker.until_next_iteration).to eq(0)
  end

  it "should give access to the uri" do
    expect(worker.uri).to match(%r(^druby://))
  end

  it "should be alive" do
    expect(worker).to be_alive
  end

  describe "when processing bucket" do
    before do
      allow(DRbObject).to receive(:new).with(nil, worker.uri).
          and_return(@worker_drb = double('drb', :alive? => true))
    end

    it "should raise an error if it is not alive via Drb" do
      allow(@worker_drb).to receive(:alive?).and_raise("drb error")
      expect {worker.start}.to raise_error(/cannot reach self/)
      allow(@worker_drb).to receive(:alive?).and_return(false)
      expect {worker.start}.to raise_error(/not alive/)
    end

    describe "" do
      before do
        allow(worker).to receive(:until_next_iteration).and_return 666
        allow(worker).to receive(:nominate)
        allow(worker).to receive(:loop?).and_return true, true, true, false
      end

      it "should relax until next iteration on MemCache errors during nomination" do
        expect(worker).to receive(:nominate).exactly(4).and_raise Dalli::DalliError.new("Buh!")
        expect(worker).to receive(:relax).with(666).exactly(4).times

        worker.start
      end

      it "should relax until next iteration on MemCache errors during request for leader" do
        expect(worker).to receive(:leader_uri).exactly(4).and_raise(Dalli::DalliError.new("Buh"))
        expect(worker).to receive(:relax).with(666).exactly(4).times

        worker.start
      end

      describe "as leader" do
        before do
          allow(worker).to receive(:leader?).and_return true
          allow(worker).to receive(:bucket_process) { sleep 0.5 }
          allow(worker).to receive(:relax)
          allow(worker).to receive(:leader).and_return(double('leader', bucket_request: nil))
        end

        it "performs leader duties in a separate thread" do
          # four iterations of 0.5 seconds → two leader threads of one second
          expect(worker).to receive(:perform_leader_duties).exactly(2).times { sleep 1 }
          worker.start
        end
      end

      describe "as follower" do
        before do
          allow(worker).to receive(:leader?).and_return false
          allow(worker).to receive(:leader_uri).and_return "the leader"
          allow(worker).to receive(:leader).and_return(@leader = double('leader'))
          allow(@leader).to receive(:bucket_request).and_return([1, 0])
        end

        it "should get the bucket to process from the leader at every iteration" do
          expect(worker).to receive(:leader).exactly(4).times.and_return @leader
          expect(@leader).to receive(:bucket_request).with(worker.uri, nil).exactly(4).times.
              and_return([1, 2])
          worker.start
        end

        it "should send the bucket_request_context with the bucket request" do
          allow(worker).to receive(:loop?).and_return false
          allow(worker).to receive(:bucket_request_context).and_return "the context"
          expect(@leader).to receive(:bucket_request).with(worker.uri, "the context")
          worker.start
        end

        it "should exit on :stop bucket" do
          expect(@leader).to receive(:bucket_request).ordered.once.and_return([:stop, 0])
          expect(worker).to receive(:exit).with(0).ordered do
            expect(worker).to receive(:loop?).and_return false
          end
          worker.start
        end
      end
    end
  end

  describe "when handling a bucket request" do
    describe "as leader" do
      before do
        allow(worker).to receive(:leader?).and_return true
      end

      it "should deliver the bucket" do
        expect(worker).to receive(:next_bucket).with("requestor", "context").and_return "the bucket"
        expect(worker.bucket_request("requestor", "context")).to eq("the bucket")
      end

      describe "when no buckets are left" do
        before do
          allow(worker).to receive(:find_workers).and_return(%w(1 2 3))
          worker.populate_followers_to_stop
          allow(DRbObject).to receive(:new).and_return(double('o', :alive? => true))
        end

        it "should deliver the :stop bucket if requestor is in followers_to_stop list" do
          expect(worker.bucket_request("1", nil)).to eq([:stop, 0])
        end

        it "should not deliver the :stop bucket if requestor is not in followers_to_stop list" do
          expect(worker.bucket_request("requestor", nil)[0]).to be_nil
        end

        it "should remove the requestor from the followers_to_stop list" do
          worker.bucket_request("2", nil)
          expect(worker.followers_to_stop).to match_array(%w(1 3))
        end
      end
    end

    describe "as follower" do
      before do
        allow(worker).to receive(:leader?).and_return false
      end

      it "should deliver the :not_leader bucket" do
        expect(worker.bucket_request("requestor", nil)[0]).to eq(:not_leader)
      end
    end
  end

  describe "when determining if restart is wanted" do
    it "should return true if the restart flag is set in memcache" do
      expect(memcache_client).to receive(:get).with('worker_restart').and_return true
      expect(worker).to be_restart_wanted
    end

    it "should return false if the restart flag is not set in memcache" do
      expect(memcache_client).to receive(:get).with('worker_restart').and_return false
      expect(worker).not_to be_restart_wanted
      expect(memcache_client).to receive(:get).with('worker_restart').and_return nil
      expect(worker).not_to be_restart_wanted
    end
  end

  describe "when performing leader duties" do
    before do
      allow(worker).to receive(:until_next_iteration).and_return 0
      allow(worker).to receive(:leader?).and_return true
      allow(worker).to receive(:dictatorship_length).and_return 666
      allow(worker).to receive(:iteration_length).and_return 5
      allow(worker).to receive(:find_workers).and_return []
      allow(worker).to receive(:initialize_buckets)
    end

    it "performs before_perform_leader_duties callback" do
      expect(worker).to receive(:before_perform_leader_duties)
      worker.perform_leader_duties
    end

    it "has a before_perform_leader_duties callback" do
      worker.send(:before_perform_leader_duties)
    end

    it "should initialize buckets as dictator" do
      expect(worker).to receive(:seize_leadership).with(666).ordered
      expect(worker).to receive(:initialize_buckets).ordered
      expect(worker).to receive(:seize_leadership).ordered
      worker.perform_leader_duties
    end

    describe "as long as there are buckets" do
      before do
        allow(worker).to receive(:buckets).and_return([1], [2], [3], [4], [])
        allow(worker).to receive(:relax)
      end

      it "should update buckets periodically" do
        expect(worker).to receive(:update_buckets).exactly(4).times
        worker.perform_leader_duties
      end

      it "should relax half of the time to the next iteration" do
        allow(worker).to receive(:until_next_iteration).and_return(6)
        expect(worker).to receive(:relax).with(3).exactly(4).times
        worker.perform_leader_duties
      end

      it "should seize the leadership periodically" do
        expect(worker).to receive(:seize_leadership).at_least(4).times
        worker.perform_leader_duties
      end

      it "should seize the leadership periodically even if restart is wanted" do
        allow(worker).to receive(:restart_wanted?).and_return true
        allow(worker).to receive(:exit)
        expect(worker).to receive(:seize_leadership).at_least(4).times
        worker.perform_leader_duties
      end

      it "should not update buckets if restart is wanted" do
        allow(worker).to receive(:restart_wanted?).and_return true
        allow(worker).to receive(:exit)
        expect(worker).not_to receive(:update_buckets)
        worker.perform_leader_duties
      end
    end

    describe "if there are no more buckets" do
      before do
        allow(worker).to receive(:buckets).and_return([])
      end

      context "when restart is wanted" do
        before do
          allow(worker).to receive(:restart_wanted?).and_return true
          allow(worker).to receive(:exit)
        end

        it "populates the followers_to_stop list before evaluating it" do
          expect(worker).to receive(:populate_followers_to_stop).ordered.once
          expect(worker).to receive(:followers_to_stop).ordered.and_return []
          worker.perform_leader_duties
        end

        context "as long as there are followers to stop" do
          before do
            allow(worker).to receive(:followers_to_stop).and_return([1], [2], [3], [4], [])
            allow(worker).to receive(:relax)
          end

          it "relaxes half of the time to the next iteration" do
            allow(worker).to receive(:until_next_iteration).and_return(6)
            expect(worker).to receive(:relax).with(3).exactly(4).times
            worker.perform_leader_duties
          end

          it "seizes the leadership periodically" do
            expect(worker).to receive(:seize_leadership).at_least(4).times
            worker.perform_leader_duties
          end
        end

        context "if there are no more followers to stop" do
          before do
            allow(worker).to receive(:followers_to_stop).and_return([])
          end

          it "exits" do
            expect(worker).to receive(:exit).with(0)
            worker.perform_leader_duties
          end
        end
      end

      context "when restart is wanted" do
        before do
          allow(worker).to receive(:restart_wanted?).and_return false
        end

        it "does not populate the followers_to_stop list if restart is not wanted" do
          allow(worker).to receive(:restart_wanted?).and_return false
          expect(worker).not_to receive(:populate_followers_to_stop)
          worker.perform_leader_duties
        end
      end
    end
  end

  describe "when seizing leadership" do
    before do
      allow(worker).to receive(:uri).and_return('myself')
      allow(worker).to receive(:iteration_length).and_return 123
      allow(worker).to receive(:token).and_return('dcc-group')
    end

    it "should set itself to leader" do
      expect(memcache_client).to receive(:set).with(anything(), 'myself', anything())
      worker.seize_leadership
    end

    it "should seize the leadership for the amount of seconds given" do
      expect(memcache_client).to receive(:set).with(anything(), anything(), 666)
      worker.seize_leadership 666
    end

    it "should seize the leadership for iteration_length if no duration is given" do
      expect(memcache_client).to receive(:set).with(anything(), anything(), 123)
      worker.seize_leadership
    end

    it "should seize the leadership for the worker's group" do
      expect(memcache_client).to receive(:set).with('dcc-group', anything(), anything())
      worker.seize_leadership
    end

    it "should have the next iteration exactly when the seized leadership ends" do
      now = Time.now
      allow(Time).to receive(:now).and_return now
      end_of_leadership = now + 666

      worker.seize_leadership 666
      expect(worker.until_next_iteration).to eq(666)

      worker.seize_leadership
      expect(worker.until_next_iteration).to eq(123)

      worker.seize_leadership 6
      expect(worker.until_next_iteration).to eq(6)
    end
  end

  describe "when creating next bucket" do
    it "should set the sleep time to sleep_until_next_bucket_time" do
      expect(worker).to receive(:sleep_until_next_bucket_time).and_return 'the sleep time'
      expect(worker.next_bucket('', nil)[1]).to eq('the sleep time')
    end
  end

  describe "when computing the sleep_until_next_bucket_time" do
    before do
      allow(worker).to receive(:iteration_length).and_return 10
      allow(worker).to receive(:until_next_iteration).and_return 6
    end

    it "should set the sleep time to half the time until_next_iteration" do
      expect(worker.sleep_until_next_bucket_time).to eq(3)
    end

    it "should set the sleep time to at least 1 second" do
      allow(worker).to receive(:until_next_iteration).and_return 0.6
      expect(worker.sleep_until_next_bucket_time).to eq(1)
    end

    it "should set the sleep time to at most a half of the interation_length" do
      allow(worker).to receive(:until_next_iteration).and_return 60
      expect(worker.sleep_until_next_bucket_time).to eq(5)
    end
  end

  describe "when providing leader object" do
    before do
      allow(worker).to receive(:until_next_iteration).and_return 0
    end

    it "should return a drb object with the leader uri" do
      allow(worker).to receive(:leader_uri).and_return("leader's uri")
      expect(DRbObject).to receive(:new).with(nil, "leader's uri").and_return "leader"
      expect(worker.leader).to eq("leader")
    end

    it "should try three times to get the leader on anarchy (no leader)" do
      expect(worker).to receive(:leader_uri).at_least(3).times.and_return nil
      worker.leader rescue nil
    end

    it "should raise an error when leader cannot be determined during anarchy" do
      allow(worker).to receive(:leader_uri).and_return nil
      expect {worker.leader}.to raise_error(/cannot determine leader/)
    end

    it "should sleep until next iteration before retrying to get leader" do
      allow(worker).to receive(:leader_uri).and_return nil
      allow(worker).to receive(:until_next_iteration).and_return 666
      expect(worker).to receive(:relax).with(666).exactly(2).times
      worker.leader rescue nil
    end
  end

  describe "when cleaning up" do
    before do
      allow(worker).to receive(:group_name).and_return('the group')
    end

    describe "as leader" do
      before do
        allow(worker).to receive(:leader?).and_return true
        allow(memcache_client).to receive(:delete)
      end

      it "should remove the leadership token from memcache" do
        expect(memcache_client).to receive(:delete).with('the group_token')
        worker.send(:internal_cleanup)
      end

      it "should remove the restart wanted flag from memcache" do
        expect(memcache_client).to receive(:delete).with('the group_restart')
        worker.send(:internal_cleanup)
      end
    end

    describe "as follower" do
      before do
        allow(worker).to receive(:leader?).and_return false
      end

      it "should not remove anything from memcache" do
        expect(memcache_client).not_to receive(:delete)
        worker.send(:internal_cleanup)
      end
    end
  end

  describe "when populating followers_to_stop" do
    before do
      allow(worker).to receive(:find_workers).and_return(%w(a b c))
      allow(DRbObject).to receive(:new).and_return(double('o', :alive? => true))
    end

    it "should add all visible workers" do
      worker.populate_followers_to_stop
      expect(worker.followers_to_stop).to match_array(%w(a b c))
    end
  end

  describe "when delivering followers_to_stop" do
    before do
      allow(worker).to receive(:find_workers).and_return(%w(a b c))
      worker.populate_followers_to_stop
      allow(DRbObject).to receive(:new).and_return(double('o', :alive? => true))
    end

    it "should return the actual followers_to_stop" do
      expect(worker.followers_to_stop).to match_array(%w(a b c))
    end

    it "should not deliver entries that are not reachable at the moment" do
      allow(DRbObject).to receive(:new).with(nil, 'a').and_return(double('o', :alive? => false))
      allow(DRbObject).to receive(:new).with(nil, 'b').and_return(x = double('o'))
      allow(x).to receive(:alive?).and_raise DRb::DRbConnError.new('nix da')
      expect(worker.followers_to_stop).to eq(%w(c))
    end

    it "should not remove unreachable entries from the list - maybe they reappear" do
      allow(DRbObject).to receive(:new).with(nil, 'a').and_return(double('o', :alive? => false))
      expect(worker.followers_to_stop).to match_array(%w(b c))
      allow(DRbObject).to receive(:new).with(nil, 'a').and_return(double('o', :alive? => true))
      expect(worker.followers_to_stop).to match_array(%w(a b c))
    end
  end
end
