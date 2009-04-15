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
      sleep 1
    end
  end
end

describe Worker, "when processing bucket" do
  before do
    @worker = Worker.new
    @worker.stub!(:until_next_iteration).and_return 666
    @worker.should_receive(:loop?).and_return true, true, true, false
  end

  it "should relax until next iteration on MemCache errors during nomination" do
    @worker.should_receive(:nominate).at_least(1).and_raise MemCache::MemCacheError.new("Buh!")
    @worker.should_receive(:relax).with(666).exactly(4).times

    @worker.start
  end

  it "should relax until next iteration on MemCache errors during request for leader" do
    @worker.should_receive(:leader_uri).at_least(1).and_raise(MemCache::MemCacheError.new("Buh!"))
    @worker.should_receive(:relax).with(666).exactly(4).times

    @worker.start
  end
end
