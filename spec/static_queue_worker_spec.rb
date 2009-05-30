# FIXME oberen Teil in spec_helper.rb auslagern
require 'rubygems'
$:.unshift(File.dirname(__FILE__) + '/../lib')
require File.dirname(__FILE__) + '/../lib/init'
Politics::log.level = Logger::FATAL

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
end

describe Worker do
  before do
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

  describe Worker, "when processing bucket" do
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
      @worker.should_receive(:leader_uri).exactly(4).and_raise(MemCache::MemCacheError.new("Buh!"))
      @worker.should_receive(:relax).with(666).exactly(4).times

      @worker.start
    end

    describe "as follower" do
      before do
        @worker.stub!(:leader?).and_return false
        @worker.stub!(:leader_uri).and_return "the leader"
      end

      it "should get the bucket to process from the leader at every iteration" do
        @worker.stub!(:uri).and_return("my uri")
        @worker.should_receive(:leader).exactly(4).times.and_return(leader = mock('leader'))
        leader.should_receive(:bucket_request).with("my uri").exactly(4).times.and_return([1, 2])
        @worker.start
      end
    end
  end

  describe Worker, "when handling a bucket request" do
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
end
