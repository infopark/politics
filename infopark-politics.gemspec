# encoding: utf-8
requirement = Gem::Requirement.new(">= 1.8")
unless requirement.satisfied_by?(Gem::Version.new(Gem::VERSION))
  raise "You need at RubyGems in Version #{requirement} to build this gem."
end

Gem::Specification.new do |gem|
  gem.name = "infopark-politics"
  gem.version = "0.9.0"
  gem.summary = "Algorithms and Tools for Distributed Computing in Ruby."
  gem.description = ""
  gem.authors = ["Mike Perham", "Tilo Prütz"]
  gem.email = "tilo@infopark.de"
  gem.homepage = "http://github.com/infopark/politics"

  gem.required_ruby_version = Gem::Requirement.new('>= 1.9')
  gem.required_rubygems_version = Gem::Requirement.new(">= 1.8")
  gem.extra_rdoc_files = [
    "LICENSE",
    "README.md"
  ]
  gem.files = `git ls-files`.split("\n")
  gem.test_files = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ["lib"]

  gem.add_development_dependency "rspec", ">= 2.14"
  gem.add_development_dependency "rake", ">= 10"
  gem.add_development_dependency "rdoc", ">= 3.9"
  gem.add_development_dependency "rubygems-tasks", ">=0.2"

  gem.add_runtime_dependency "dalli"
end

