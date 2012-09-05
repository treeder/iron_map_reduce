require 'iron_mq'
require 'iron_cache'
require 'iron_worker_ng'

cache_name = params[:cache_name]
f = params[:cache_key]

use_files = false
@ic = IronCache::Client.new(token: params[:token], project_id: params[:project_id])
@mq = IronMQ::Client.new(token: params[:token], project_id: params[:project_id])
@worker = IronWorkerNG::Client.new(token: params[:token], project_id: params[:project_id])

cache = @ic.cache(cache_name)

s = cache.get(f).value

def map(s)
  @word_counts = {}

  s.each_line do |line|
    words = line.split
    words.each do |word|
      word = word.gsub(/[,()'"]/, '')
      if @word_counts[word]
        @word_counts[word] += 1
      else
        @word_counts[word] = 1
      end
    end
  end

  puts "Words count: "
  num_less_than_1000 = 0
  @word_counts.sort { |a, b| a[1] <=> b[1] }.each do |key, value|
    if value < 1000
      num_less_than_1000 += 1
    else
      puts "#{key} => #{value}"
    end
  end
  puts "#{num_less_than_1000} less than 1000"

  @word_counts

end

# Run Map operation
word_counts = map(s)

# Several options to store results, one using cache, one using queue
# Option 1) Increment in Cache (probably a bit too specific for counting)
word_counts.each_pair do |word, count|
  cache_key = "word_count_#{word}"
  begin
    cache.increment(cache_key, count, :expires_in => 60*10)
  rescue IronCore::ResponseError => ex
    if ex.code == 404
      # key not found, so let's make it
      cache.put(cache_key, 1, :expires_in => 60*10)
    end
  end

  # Option 2) Put results in a message queue
  #this should perhaps be separate queue for each word?
  @mq.queue("words").post({word: word, cache_key: cache_key, count: count}.to_json)
end
# todo: put results in cache for each map job
# todo: put count for a word in a message queue?  Then reducer looks at queues and pulls numbers off the queue
# todo: OR increment a cache entry?
