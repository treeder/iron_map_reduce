require_relative 'chunker'
require_relative 'mapper'
require_relative 'reducer'
require 'iron_mq'
require 'iron_cache'
require 'iron_worker_ng'


class WordCount


  def reduce(word, values)
    sum = 0
    values.each do |v|
      sum += v
    end
    sum
  end

end


use_files = false
@ic = IronCache::Client.new
@mq = IronMQ::Client.new
@worker = IronWorkerNG::Client.new
input_dir = "input"
cache_name = "chunks"

chunker = Chunker.new
if use_files
  files = chunker.file_chunker "http://norvig.com/big.txt", "big", 1024*1024, input_dir
else
  cache = @ic.cache(cache_name)
  files = chunker.iron_chunker "http://norvig.com/big.txt", "big", cache, :chunksize => 1024*900, :expires_in => 60*10
end

# todo: write files to a separate entry to pull up list for mapper

# todo: get file list from remote location. Cache or maybe MQ?

tasks = []
wc = WordCount.new
files.each_with_index do |f, i|
  # todo: queue each of these up as a worker. keep a list of the worker id's to get status' and restart any that errored out
  task = @worker.tasks.create('word_count_mapper', cache_name: cache_name, cache_key: f, token: @worker.token, project_id: @worker.project_id)
  tasks << task
  # todo: wait until all tasks are done before continuing to next step. Bail or retry on errors?


end

while true
  sleep 10
  puts "checking task statuses"
  tasks.each do |t|
    t2 = @worker.tasks.get(t.id)
    if t2.status == "error"
      raise "Error: #{t2.msg} Log: #{@worker.tasks.log(t.id)}"
    end
    if t2.status == "running" || t2.status == "queued"
      next
    end
  end
end

puts "All map jobs are done"

# Now for reduce
# Now pull off queue to tally up results
#queue = @mq.queue("words")
#queue.poll do |msg|
#  msg = JSON.parse(msg)
#  word = msg['word']
#
#end



# todo: put list of word count locations in cache entry

# todo: get list of word count locations from cache


#words.each_with_index do |wc, i|
#
#  wc.reduce
#end

# should also do LineIndexer

class LineIndexer
  def map(file_path)
    @word_counts = {}
    file = open(file_path, "r")
    file.each_line do |line|
      words = line.split
      words.each do |word|
        word = word.gsub(/[,()'"]/, '')
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




