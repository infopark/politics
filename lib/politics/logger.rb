require 'logger'

class Logger
  def context(tag)
    tags.push(tag)
    yield
  ensure
    tags.pop
  end

  private

  def formatter
    proc do |severity, datetime, progname, msg|
      "#{severity[0].upcase}"\
          " [#{datetime.strftime("%Y-%m-%d %H:%M:%S.%3N")} #{Process.pid}] #{progname}:"\
          "#{
            unless tags.empty
              " [#{tags.join("][")}]"
            end
          } #{msg}"
    end
  end

  def tags
    @tags ||= []
  end
end
