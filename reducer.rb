
require 'open-uri'
class Reducer

  int sum = 0;
  while (values.hasNext()) {
      IntWritable value = (IntWritable) values.next();
  sum += value.get(); // process value
  }

  output.collect(key, new IntWritable(sum));

end