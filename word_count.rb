require_relative 'chunker'
require_relative 'mapper'
require_relative 'reducer'
require 'iron_mq'
require 'iron_cache'


class WordCount

  def map(file_path)
    @word_counts = {}
    file = open(file_path, "r")
    file.each_line do |line|
      words = line.split
      words.each do |word|
        word = word.gsub(/[,()'"]/,'')
        if @word_counts[word]
          @word_counts[word] += 1
        else
          @word_counts[word] = 1
        end
      end
    end

    puts "Words count: "
    num_less_than_1000 = 0
    @word_counts.sort {|a,b| a[1] <=> b[1]}.each do |key,value|
      if value < 1000
        num_less_than_1000 += 1
      else
        puts "#{key} => #{value}"
      end
    end
    puts "#{num_less_than_1000} less than 1000"

    @word_counts

  end

  def reduce(word, values)
    sum = 0
    values.each do |v|
      sum += v
    end
    sum
  end

end

@ic = IronCache::Client.new
@mq = IronMQ::Client.new
input_dir = "input"

chunker = Chunker.new
files = chunker.iron_chunker "http://norvig.com/big.txt", "big", 64000, input_dir

# todo: Store files somewhere online - IronCache would be nice, could try tons of small chunks.
# todo: write files to a separate entry to pull up list for mapper

# todo: get file list from remote location. Cache or maybe MQ?

wc = WordCount.new
word_counts = []
files.each_with_index do |f, i|
  # todo: this should be a worker, keep a list of the worker id's to get status' and restart any that errored out
  word_counts << wc.map("#{input_dir}/#{f}")
  # todo: put results in cache for each map job
  # todo: put count for a word in a message queue?  Then reducer looks at queues and pulls numbers off the queue
  # todo: OR increment a cache entry?
  # todo: increment a counter in cache so we'll know when to run the rest and start the reducers, or just check all worker status' (probably better)
end
# todo: put list of word count locations in cache entry

# todo: get list of word count locations from cache
words.each_with_index do |wc, i|

  wc.reduce
end

# should also do LineIndexer

class LineIndexer
  def map(file_path)
    @word_counts = {}
    file = open(file_path, "r")
    file.each_line do |line|
      words = line.split
      words.each do |word|
        word = word.gsub(/[,()'"]/,'')
        if @word_counts[word]
          @word_counts[word] << file_path
        else
          @word_counts[word] = [file_path]
        end
      end
    end
    puts "Indexed #{file_path}"
    @word_counts
  end

  def reduce(word, values)
    contained_in = ""
    first = true
    values.each do |v|
      if !first
        contained_in << ","
      end
      first = false
      contained_in << v
    end
    contained_in
  end
end




