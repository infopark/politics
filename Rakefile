# encoding: utf-8
require 'rdoc/task'
require 'rake/testtask'
require 'rspec/core/rake_task'
require 'rubygems/tasks'

Gem::Tasks.new

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
