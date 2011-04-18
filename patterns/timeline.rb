class TimelinePattern
    def initialize(redis,lowlevel,highlevel)
        @r = redis
        @lowlevel = lowlevel
        @highlevel = highlevel
    end

    def insert(list,event)
        count = @r.lpush(list,event)
        @r.ltrim(list,0,@lowlevel-1) if count >= @highlevel
    end

    def remove(list,event)
        @r.lrem(list,1,event)
    end

    def fetch(list,count=10,start=0)
        @r.lrange(list,start,start+count-1)
    end
end

# Usage
timeline = TimelinePattern.new(Redis.new,100,110)
timeline.insert("foo")
timeline.insert("bar")
timeline.fetch(list)
