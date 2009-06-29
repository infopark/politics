# FIXME oberen Teil in spec_helper.rb auslagern
require 'rubygems'
$:.unshift(File.dirname(__FILE__) + '/../lib')
require File.dirname(__FILE__) + '/../lib/init'
Politics::log.level = Logger::FATAL

@@memcache_client = nil

class Worker
  include Politics::StaticQueueWorker
  def initialize
    log.level = Logger::FATAL
    register_worker 'worker', 10, :iteration_length => 10
  end

  def start
    process_bucket do |bucket|
      sleep 0.1
    end
  end

  def client_for(servers)
    @@memcache_client
  end
end

describe Worker do
  before do
    @@memcache_client = mock('memcache', :set => nil, :get => nil)
    @worker = Worker.new
  end

  it "it should provide 'until_next_iteration' even if nominate was not completed" do
    @worker.until_next_iteration
  end

  it "should return time to next iteration even if nominate was not completed" do
    @worker.until_next_iteration.should > 0
    @worker.until_next_iteration.should <= 10
  end

  it "should give access to the uri" do
    @worker.uri.should =~ %r(^druby://)
  end

  it "should be alive" do
    @worker.should be_alive
  end

  describe "when processing bucket" do
    before do
      DRbObject.stub!(:new).with(nil, @worker.uri).
          and_return(@worker_drb = mock('drb', :alive? => true))
    end

    it "should raise an error if it is not alive via Drb" do
      @worker_drb.stub!(:alive?).and_raise("drb error")
      lambda {@worker.start}.should raise_error(/cannot reach self/)
      @worker_drb.stub!(:alive?).and_return(false)
      lambda {@worker.start}.should raise_error(/not alive/)
    end

    describe "" do
      before do
        @worker.stub!(:until_next_iteration).and_return 666
        @worker.stub!(:nominate)
        @worker.should_receive(:loop?).and_return true, true, true, false
      end

      it "should relax until next iteration on MemCache errors during nomination" do
        @worker.should_receive(:nominate).exactly(4).and_raise MemCache::MemCacheError.new("Buh!")
        @worker.should_receive(:relax).with(666).exactly(4).times

        @worker.start
      end

      it "should relax until next iteration on MemCache errors during request for leader" do
        @worker.should_receive(:leader_uri).exactly(4).and_raise(MemCache::MemCacheError.new("Buh"))
        @worker.should_receive(:relax).with(666).exactly(4).times

        @worker.start
      end

      describe "as leader" do
        before do
          @worker.stub!(:leader?).and_return true
        end

        it "should do leader duties" do
          @worker.should_receive(:perform_leader_duties).exactly(4).times
          @worker.start
        end
      end

      describe "as follower" do
        before do
          @worker.stub!(:leader?).and_return false
          @worker.stub!(:leader_uri).and_return "the leader"
        end

        it "should get the bucket to process from the leader at every iteration" do
          @worker.should_receive(:leader).exactly(4).times.and_return(leader = mock('leader'))
          leader.should_receive(:bucket_request).with(@worker.uri).exactly(4).times.
              and_return([1, 2])
          @worker.start
        end
      end
    end
  end

  describe "when handling a bucket request" do
    describe "as leader" do
      before do
        @worker.stub!(:leader?).and_return true
      end

      it "should deliver the bucket" do
        @worker.should_receive(:next_bucket).with("requestor").and_return "the bucket"
        @worker.bucket_request("requestor").should == "the bucket"
      end
    end

    describe "as follower" do
      before do
        @worker.stub!(:leader?).and_return false
      end

      it "should deliver the :not_leader bucket" do
        @worker.bucket_request("requestor")[0].should == :not_leader
      end
    end
  end

  describe "when performing leader duties" do
    before do
      @worker.stub!(:until_next_iteration).and_return 0
      @worker.stub!(:leader?).and_return true
      @worker.stub!(:dictatorship_length).and_return 666
      @worker.stub!(:iteration_length).and_return 5
    end

    it "should initialize buckets as dictator" do
      @worker.should_receive(:seize_leadership).with(666).ordered
      @worker.should_receive(:initialize_buckets).ordered
      @worker.should_receive(:seize_leadership).ordered
      @worker.perform_leader_duties
    end

    describe "as long as there are buckets" do
      before do
        @worker.stub!(:buckets).and_return([1], [2], [3], [4], [])
        @worker.stub!(:relax)
      end

      it "should update buckets periodically" do
        @worker.should_receive(:update_buckets).exactly(4).times
        @worker.perform_leader_duties
      end

      it "should relax half of the time to the next iteration" do
        @worker.stub!(:until_next_iteration).and_return(6)
        @worker.should_receive(:relax).with(3).exactly(4).times
        @worker.perform_leader_duties
      end

      it "should seize the leadership periodically" do
        @worker.should_receive(:seize_leadership).at_least(4).times
        @worker.perform_leader_duties
      end
    end

    describe "if there are no more buckets" do
      before do
        @worker.stub!(:buckets).and_return([])
      end

      it "should relax until next iteration" do
        @worker.stub!(:until_next_iteration).and_return(6)
        @worker.should_receive(:relax).with(6).once
        @worker.perform_leader_duties
      end
    end
  end

  describe "when seizing leadership" do
    before do
      @worker.stub!(:uri).and_return('myself')
      @worker.stub!(:iteration_length).and_return 123
      @worker.stub!(:token).and_return('dcc-group')
    end

    it "should set itself to leader" do
      @@memcache_client.should_receive(:set).with(anything(), 'myself', anything())
      @worker.seize_leadership
    end

    it "should seize the leadership for the amount of seconds given" do
      @@memcache_client.should_receive(:set).with(anything(), anything(), 666)
      @worker.seize_leadership 666
    end

    it "should seize the leadership for iteration_length if no duration is given" do
      @@memcache_client.should_receive(:set).with(anything(), anything(), 123)
      @worker.seize_leadership
    end

    it "should seize the leadership for the worker's group" do
      @@memcache_client.should_receive(:set).with('dcc-group', anything(), anything())
      @worker.seize_leadership
    end

    it "should have the next iteration exactly when the seized leadership ends" do
      now = Time.now
      Time.stub!(:now).and_return now
      end_of_leadership = now + 666

      @worker.seize_leadership 666
      @worker.until_next_iteration.should == 666

      @worker.seize_leadership
      @worker.until_next_iteration.should == 123

      @worker.seize_leadership 6
      @worker.until_next_iteration.should == 6
    end
  end
end
