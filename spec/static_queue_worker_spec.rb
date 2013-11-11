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
      worker.should_receive(:at_exit).ordered.and_return {|&h| h}
      handler = worker.register_worker('worker', 10, :iteration_length => 10)

      worker.should_receive(:cleanup).ordered
      handler.call
    end

    it "should have a druby url" do
      worker.register_worker('worker', 10, :iteration_length => 10)
      worker.uri.should =~ %r|druby://.*:[0-9]+|
    end

    it "should not have a hostname" do
      worker.send(:hostname).should be_nil
    end

    context "when it has a hostname" do
      before do
        worker.stub(:hostname).and_return '127.0.0.1'
      end

      it "should use it" do
        worker.register_worker('worker', 10, :iteration_length => 10)
        worker.uri.should =~ %r|druby://127.0.0.1:[0-9]+|
      end
    end

    context "when it does not have a hostname" do
      before do
        worker.stub(:hostname).and_return nil
      end

      it "should use the systems hostname" do
        worker.register_worker('worker', 10, :iteration_length => 10)
        worker.uri.should =~ %r|druby://localhost:[0-9]+|
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
    worker.until_next_iteration.should > 0
    worker.until_next_iteration.should <= 10
  end

  it "should give access to the uri" do
    worker.uri.should =~ %r(^druby://)
  end

  it "should be alive" do
    worker.should be_alive
  end

  describe "when processing bucket" do
    before do
      DRbObject.stub(:new).with(nil, worker.uri).
          and_return(@worker_drb = double('drb', :alive? => true))
    end

    it "should raise an error if it is not alive via Drb" do
      @worker_drb.stub(:alive?).and_raise("drb error")
      lambda {worker.start}.should raise_error(/cannot reach self/)
      @worker_drb.stub(:alive?).and_return(false)
      lambda {worker.start}.should raise_error(/not alive/)
    end

    describe "" do
      before do
        worker.stub(:until_next_iteration).and_return 666
        worker.stub(:nominate)
        worker.stub(:loop?).and_return true, true, true, false
      end

      it "should relax until next iteration on MemCache errors during nomination" do
        worker.should_receive(:nominate).exactly(4).and_raise MemCache::MemCacheError.new("Buh!")
        worker.should_receive(:relax).with(666).exactly(4).times

        worker.start
      end

      it "should relax until next iteration on MemCache errors during request for leader" do
        worker.should_receive(:leader_uri).exactly(4).and_raise(MemCache::MemCacheError.new("Buh"))
        worker.should_receive(:relax).with(666).exactly(4).times

        worker.start
      end

      describe "as leader" do
        before do
          worker.stub(:leader?).and_return true
        end

        it "should do leader duties" do
          worker.should_receive(:perform_leader_duties).exactly(4).times
          worker.start
        end
      end

      describe "as follower" do
        before do
          worker.stub(:leader?).and_return false
          worker.stub(:leader_uri).and_return "the leader"
          worker.stub(:leader).and_return(@leader = double('leader'))
          @leader.stub(:bucket_request).and_return([1, 0])
        end

        it "should get the bucket to process from the leader at every iteration" do
          worker.should_receive(:leader).exactly(4).times.and_return @leader
          @leader.should_receive(:bucket_request).with(worker.uri).exactly(4).times.
              and_return([1, 2])
          worker.start
        end

        it "should exit on :stop bucket" do
          @leader.should_receive(:bucket_request).ordered.once.and_return([:stop, 0])
          worker.should_receive(:exit).with(0).ordered.and_return do
            worker.should_receive(:loop?).and_return false
          end
          worker.start
        end
      end
    end
  end

  describe "when handling a bucket request" do
    describe "as leader" do
      before do
        worker.stub(:leader?).and_return true
      end

      it "should deliver the bucket" do
        worker.should_receive(:next_bucket).with("requestor").and_return "the bucket"
        worker.bucket_request("requestor").should == "the bucket"
      end

      describe "when no buckets are left" do
        before do
          worker.stub(:find_workers).and_return(%w(1 2 3))
          worker.populate_followers_to_stop
          DRbObject.stub(:new).and_return(double('o', :alive? => true))
        end

        it "should deliver the :stop bucket if requestor is in followers_to_stop list" do
          worker.bucket_request("1").should == [:stop, 0]
        end

        it "should not deliver the :stop bucket if requestor is not in followers_to_stop list" do
          worker.bucket_request("requestor")[0].should be_nil
        end

        it "should remove the requestor from the followers_to_stop list" do
          worker.bucket_request("2")
          worker.followers_to_stop.should =~ %w(1 3)
        end
      end
    end

    describe "as follower" do
      before do
        worker.stub(:leader?).and_return false
      end

      it "should deliver the :not_leader bucket" do
        worker.bucket_request("requestor")[0].should == :not_leader
      end
    end
  end

  describe "when determining if restart is wanted" do
    it "should return true if the restart flag is set in memcache" do
      memcache_client.should_receive(:get).with('worker_restart').and_return true
      worker.should be_restart_wanted
    end

    it "should return false if the restart flag is not set in memcache" do
      memcache_client.should_receive(:get).with('worker_restart').and_return false
      worker.should_not be_restart_wanted
      memcache_client.should_receive(:get).with('worker_restart').and_return nil
      worker.should_not be_restart_wanted
    end
  end

  describe "when performing leader duties" do
    before do
      worker.stub(:until_next_iteration).and_return 0
      worker.stub(:leader?).and_return true
      worker.stub(:dictatorship_length).and_return 666
      worker.stub(:iteration_length).and_return 5
      worker.stub(:find_workers).and_return []
    end

    it "should initialize buckets as dictator" do
      worker.should_receive(:seize_leadership).with(666).ordered
      worker.should_receive(:initialize_buckets).ordered
      worker.should_receive(:seize_leadership).ordered
      worker.perform_leader_duties
    end

    describe "as long as there are buckets" do
      before do
        worker.stub(:buckets).and_return([1], [2], [3], [4], [])
        worker.stub(:relax)
      end

      it "should update buckets periodically" do
        worker.should_receive(:update_buckets).exactly(4).times
        worker.perform_leader_duties
      end

      it "should relax half of the time to the next iteration" do
        worker.stub(:until_next_iteration).and_return(6)
        worker.should_receive(:relax).with(3).exactly(4).times
        worker.perform_leader_duties
      end

      it "should seize the leadership periodically" do
        worker.should_receive(:seize_leadership).at_least(4).times
        worker.perform_leader_duties
      end

      it "should seize the leadership periodically even if restart is wanted" do
        worker.stub(:restart_wanted?).and_return true
        worker.stub(:exit)
        worker.should_receive(:seize_leadership).at_least(4).times
        worker.perform_leader_duties
      end

      it "should not update buckets if restart is wanted" do
        worker.stub(:restart_wanted?).and_return true
        worker.stub(:exit)
        worker.should_not_receive(:update_buckets)
        worker.perform_leader_duties
      end
    end

    describe "if there are no more buckets" do
      before do
        worker.stub(:buckets).and_return([])
      end

      it "should populate the followers_to_stop list before evaluating it if restart is wanted" do
        worker.stub(:restart_wanted?).and_return true
        worker.stub(:exit)
        worker.should_receive(:populate_followers_to_stop).ordered.once
        worker.should_receive(:followers_to_stop).ordered.and_return []
        worker.perform_leader_duties
      end

      it "should not populate the followers_to_stop list if restart is not wanted" do
        worker.stub(:restart_wanted?).and_return false
        worker.should_not_receive(:populate_followers_to_stop)
        worker.perform_leader_duties
      end

      describe "as long as there are followers to stop" do
        before do
          worker.stub(:followers_to_stop).and_return([1], [2], [3], [4], [])
          worker.stub(:relax)
        end

        it "should relax half of the time to the next iteration" do
          worker.stub(:until_next_iteration).and_return(6)
          worker.should_receive(:relax).with(3).exactly(4).times
          worker.perform_leader_duties
        end

        it "should seize the leadership periodically" do
          worker.should_receive(:seize_leadership).at_least(4).times
          worker.perform_leader_duties
        end
      end

      describe "if there are no more followers to stop" do
        before do
          worker.stub(:followers_to_stop).and_return([])
        end

        it "should relax until next iteration" do
          worker.stub(:until_next_iteration).and_return(6)
          worker.should_receive(:relax).with(6).once
          worker.perform_leader_duties
        end

        it "should exit if restart is wanted" do
          worker.stub(:restart_wanted?).and_return true
          worker.should_receive(:exit).with(0)
          worker.perform_leader_duties
        end
      end
    end
  end

  describe "when seizing leadership" do
    before do
      worker.stub(:uri).and_return('myself')
      worker.stub(:iteration_length).and_return 123
      worker.stub(:token).and_return('dcc-group')
    end

    it "should set itself to leader" do
      memcache_client.should_receive(:set).with(anything(), 'myself', anything())
      worker.seize_leadership
    end

    it "should seize the leadership for the amount of seconds given" do
      memcache_client.should_receive(:set).with(anything(), anything(), 666)
      worker.seize_leadership 666
    end

    it "should seize the leadership for iteration_length if no duration is given" do
      memcache_client.should_receive(:set).with(anything(), anything(), 123)
      worker.seize_leadership
    end

    it "should seize the leadership for the worker's group" do
      memcache_client.should_receive(:set).with('dcc-group', anything(), anything())
      worker.seize_leadership
    end

    it "should have the next iteration exactly when the seized leadership ends" do
      now = Time.now
      Time.stub(:now).and_return now
      end_of_leadership = now + 666

      worker.seize_leadership 666
      worker.until_next_iteration.should == 666

      worker.seize_leadership
      worker.until_next_iteration.should == 123

      worker.seize_leadership 6
      worker.until_next_iteration.should == 6
    end
  end

  describe "when creating next bucket" do
    it "should set the sleep time to sleep_until_next_bucket_time" do
      worker.should_receive(:sleep_until_next_bucket_time).and_return 'the sleep time'
      worker.next_bucket('')[1].should == 'the sleep time'
    end
  end

  describe "when computing the sleep_until_next_bucket_time" do
    before do
      worker.stub(:iteration_length).and_return 10
      worker.stub(:until_next_iteration).and_return 6
    end

    it "should set the sleep time to half the time until_next_iteration" do
      worker.sleep_until_next_bucket_time.should == 3
    end

    it "should set the sleep time to at least 1 second" do
      worker.stub(:until_next_iteration).and_return 0.6
      worker.sleep_until_next_bucket_time.should == 1
    end

    it "should set the sleep time to at most a half of the interation_length" do
      worker.stub(:until_next_iteration).and_return 60
      worker.sleep_until_next_bucket_time.should == 5
    end
  end

  describe "when providing leader object" do
    before do
      worker.stub(:until_next_iteration).and_return 0
    end

    it "should return a drb object with the leader uri" do
      worker.stub(:leader_uri).and_return("leader's uri")
      DRbObject.should_receive(:new).with(nil, "leader's uri").and_return "leader"
      worker.leader.should == "leader"
    end

    it "should try three times to get the leader on anarchy (no leader)" do
      worker.should_receive(:leader_uri).at_least(3).times.and_return nil
      worker.leader rescue nil
    end

    it "should raise an error when leader cannot be determined during anarchy" do
      worker.stub(:leader_uri).and_return nil
      lambda {worker.leader}.should raise_error(/cannot determine leader/)
    end

    it "should sleep until next iteration before retrying to get leader" do
      worker.stub(:leader_uri).and_return nil
      worker.stub(:until_next_iteration).and_return 666
      worker.should_receive(:relax).with(666).exactly(2).times
      worker.leader rescue nil
    end
  end

  describe "when cleaning up" do
    before do
      worker.stub(:group_name).and_return('the group')
    end

    describe "as leader" do
      before do
        worker.stub(:leader?).and_return true
        memcache_client.stub(:delete)
      end

      it "should remove the leadership token from memcache" do
        memcache_client.should_receive(:delete).with('the group_token')
        worker.send(:cleanup)
      end

      it "should remove the restart wanted flag from memcache" do
        memcache_client.should_receive(:delete).with('the group_restart')
        worker.send(:cleanup)
      end
    end

    describe "as follower" do
      before do
        worker.stub(:leader?).and_return false
      end

      it "should not remove anything from memcache" do
        memcache_client.should_not_receive(:delete)
        worker.send(:cleanup)
      end
    end
  end

  describe "when populating followers_to_stop" do
    before do
      worker.stub(:find_workers).and_return(%w(a b c))
      DRbObject.stub(:new).and_return(double('o', :alive? => true))
    end

    it "should add all visible workers" do
      worker.populate_followers_to_stop
      worker.followers_to_stop.should =~ %w(a b c)
    end
  end

  describe "when delivering followers_to_stop" do
    before do
      worker.stub(:find_workers).and_return(%w(a b c))
      worker.populate_followers_to_stop
      DRbObject.stub(:new).and_return(double('o', :alive? => true))
    end

    it "should return the actual followers_to_stop" do
      worker.followers_to_stop.should =~ %w(a b c)
    end

    it "should not deliver entries that are not reachable at the moment" do
      DRbObject.stub(:new).with(nil, 'a').and_return(double('o', :alive? => false))
      DRbObject.stub(:new).with(nil, 'b').and_return(x = double('o'))
      x.stub(:alive?).and_raise DRb::DRbConnError.new('nix da')
      worker.followers_to_stop.should == %w(c)
    end

    it "should not remove unreachable entries from the list - maybe they reappear" do
      DRbObject.stub(:new).with(nil, 'a').and_return(double('o', :alive? => false))
      worker.followers_to_stop.should =~ %w(b c)
      DRbObject.stub(:new).with(nil, 'a').and_return(double('o', :alive? => true))
      worker.followers_to_stop.should =~ %w(a b c)
    end
  end
end
