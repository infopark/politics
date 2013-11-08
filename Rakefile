# encoding: utf-8
require 'rdoc/task'
require 'rake/testtask'
require 'rspec/core/rake_task'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "infopark-politics"
    gemspec.summary = "Algorithms and Tools for Distributed Computing in Ruby."
    gemspec.email = "tilo@infopark.de"
    gemspec.homepage = "http://github.com/infopark/politics"
    gemspec.description = gemspec.summary
    gemspec.authors = ["Mike Perham", "Tilo Prütz"]
    gemspec.add_dependency 'memcache-client', '>= 1.5.0'
    gemspec.add_dependency 'starling-starling', '>=0.9.8'
    gemspec.add_dependency 'net-mdns', '>=0.4'
    gemspec.extra_rdoc_files.include "History.rdoc"
    gemspec.files = FileList["{lib,examples}/**/*"]
    #gemspec.test_files.exclude 'examples/**/*'
  end
  Jeweler::GemcutterTasks.new
end

desc "Run tests"
Rake::TestTask.new do |t|
  t.libs << 'test' << 'lib'
  t.test_files = FileList['test/*_test.rb']
end

desc "Run specs"
RSpec::Core::RakeTask.new

desc "Create rdoc"
Rake::RDocTask.new do |rd|
  rd.main = "README.rdoc"
  rd.rdoc_files.include("README.rdoc", "History.rdoc", "lib/**/*.rb")
end


task :default => :test
