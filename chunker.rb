require 'rest'
require 'open-uri'
require 'fileutils'

class Chunker

  def count_words_without_loops(string)
    res = Hash.new(0)
    string.downcase.scan(/\w+/).map { |word| res[word] = string.downcase.scan(/\b#{word}\b/).size }
    return res
  end


  def iron_chunker(f_in, cache, chunksize = 64000)
    outfilenum = 0
    # Can also load local file with same code
    files_written = []
    FileUtils.mkdir_p(output_dir)
    open(f_in, "r") do |fh_in|
      until fh_in.eof?
        output_f_name = "#{out_pref}_#{outfilenum}.txt"
        files_written << output_f_name
        value = ""
        line = ""
        while value.size <= (chunksize-line.length) && !fh_in.eof?
          line = fh_in.readline
          value << line
        end
        cache.put("#{output_f_name}", value)
        outfilenum += 1
      end
    end
    files_written

  end

  def file_chunker(f_in, out_pref, chunksize = 1024*1024, output_dir="input")
    outfilenum = 0
    # Can also load local file with same code
    files_written = []
    FileUtils.mkdir_p(output_dir)
    open(f_in, "r") do |fh_in|
      until fh_in.eof?
        output_f_name = "#{out_pref}_#{outfilenum}.txt"
        files_written << output_f_name
        File.open("#{output_dir}/#{output_f_name}", "w") do |fh_out|
          line = ""
          while fh_out.size <= (chunksize-line.length) && !fh_in.eof?
            line = fh_in.readline
            fh_out << line
          end
        end
        outfilenum += 1
      end
    end
    files_written

  end

end

#rest = Rest::Client.new
#resp = rest.get("http://norvig.com/big.txt")
#txt = resp.body

#hash = count_words_without_loops(txt)
#p hash.size
