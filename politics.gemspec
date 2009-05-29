# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{politics}
  s.version = "0.2.6"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Mike Perham", "Tilo Pr\303\274tz"]
  s.date = %q{2009-05-29}
  s.description = %q{Algorithms and Tools for Distributed Computing in Ruby.}
  s.email = %q{tilo@infopark.de}
  s.extra_rdoc_files = [
    "History.rdoc",
     "LICENSE",
     "README.rdoc"
  ]
  s.files = [
    "examples/queue_worker_example.rb",
     "examples/token_worker_example.rb",
     "lib/init.rb",
     "lib/politics.rb",
     "lib/politics/discoverable_node.rb",
     "lib/politics/static_queue_worker.rb",
     "lib/politics/token_worker.rb",
     "lib/politics/version.rb"
  ]
  s.has_rdoc = true
  s.homepage = %q{http://github.com/infopark/politics}
  s.rdoc_options = ["--charset=UTF-8"]
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.1}
  s.summary = %q{Algorithms and Tools for Distributed Computing in Ruby.}
  s.test_files = [
    "spec/static_queue_worker_spec.rb",
     "test/test_helper.rb",
     "test/static_queue_worker_test.rb",
     "test/token_worker_test.rb"
  ]

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 2

    if Gem::Version.new(Gem::RubyGemsVersion) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<memcache-client>, [">= 1.5.0"])
      s.add_runtime_dependency(%q<starling-starling>, [">= 0.9.8"])
      s.add_runtime_dependency(%q<net-mdns>, [">= 0.4"])
    else
      s.add_dependency(%q<memcache-client>, [">= 1.5.0"])
      s.add_dependency(%q<starling-starling>, [">= 0.9.8"])
      s.add_dependency(%q<net-mdns>, [">= 0.4"])
    end
  else
    s.add_dependency(%q<memcache-client>, [">= 1.5.0"])
    s.add_dependency(%q<starling-starling>, [">= 0.9.8"])
    s.add_dependency(%q<net-mdns>, [">= 0.4"])
  end
end
