-- module counter - counts some junk

local today = os.date('*t')
local timestamp = os.time{year = today['year'], month = today['month'], day = today['day']}
local referer = '-';

if ngx.var.http_referer ~= nil then
	_, _, referer = string.find(ngx.var.http_referer, '^https?://([%w%.]+)')
end

local key = 'stat_counter_' .. timestamp .. '_' .. referer

-- connect to redis
local redis = require 'resty.redis'
local red = redis:new()
local ok, err = red:connect('127.0.0.1', 6379)
if not ok then
	ngx.log(ngx.ERR, 'redis connection failed: ', err)
	ngx.exit(500)
end

local cjson = require 'cjson'

-- decodes values from array into dict
function decode_bulk(reply)
	local data = {}
	for j=1, #reply, 2 do
		data[reply[j] ] = reply[j+1]
	end
	return(data)
end

local data, empty
empty, err = red:hsetnx(key, 'today', 0)
if not empty then
	ngx.log(ngx.ERR, 'redis query failed: ', err)
	ngx.exit(500)
end

if empty == 0 then
	data, err = red:hgetall(key)
	if not data then
		ngx.log(ngx.ERR, 'redis query failed: ', err)
		ngx.exit(500)
	end
end

if empty == 1 or data.whole == nil then
	-- postgres fallback
	result = ngx.location.capture('/postgres', {
		method = ngx.HTTP_PUT,
		body = "select * from get_stats('" .. referer .. "') as (today int, lastday int, week bigint, whole bigint);"
	})
	if result.status ~= 200 or not result.body then
		ngx.log(ngx.ERR, 'postgres access failed')
		ngx.exit(500)
	else
		local unrds = require "rds.parser"
		local res, err = unrds.parse(result.body)
		if res == nil then
			ngx.log(ngx.ERR, 'failed to obtain data: ' .. err)
			ngx.exit(500)
		else
			data = res.resultset[1]
			local req = {key}

			for name, value in pairs(data) do
				if value == unrds.null then
					value = 0
				end
				if name ~= 'today' then
					table.insert(req, name)
					table.insert(req, value)
				end
			end

			red:init_pipeline(3)
			red:hmset{req}
			red:expire(key, 129600)
			red:hincrby(key, 'today', data.today)
			res, err = red:commit_pipeline()
			if not res then
				ngx.log(ngx.ERR, 'redis pipeline failed: ', err)
				ngx.exit(500)
			end
		end
	end
end

data.today = data.today + 1
ngx.say(cjson.encode(data))
ngx.eof()

res, err = red:hincrby(key, 'today', 1)
local uid = ''

if ngx.var.uid_got ~= nil and string.find(ngx.var.uid_got, 'uid=') == 1 then
	uid = string.sub(ngx.var.uid_got, 5)
elseif ngx.var.uid_set ~= nil and string.find(ngx.var.uid_set, 'uid=') == 1 then
	uid = string.sub(ngx.var.uid_set, 5)
end

local hit_key = uid .. '_' .. referer .. '_' .. ngx.var.remote_addr

red:init_pipeline(5)
red:multi()
red:hincrby('stat_counter_pending', hit_key, 1)
red:expire('stat_counter_pending', 60)
red:renamenx('stat_counter_pending', 'stat_counter_pending_tmp')
red:exec()
renamed, err = red:commit_pipeline()
if not renamed then
	ngx.log(ngx.ERR, 'redis multi failed: ', err)
	ngx.exit(500)
end

local continue = false
if tonumber(renamed[5][3]) == 1 then
	continue = true
end

while continue do
	ngx.location.capture('/sleep')

	local data
	data, err = red:hgetall('stat_counter_pending_tmp')
	if not data then
		ngx.log(ngx.ERR, 'redis request failed: ', err)
		ngx.exit(500)
	end

	for name, value in pairs(decode_bulk(data)) do
		local fields = {}
		name:gsub("[^_]+", function(c) fields[#fields + 1] = c end)
		fields[#fields + 1] = value
		local result = ngx.location.capture('/postgres', {
			method = ngx.HTTP_PUT,
			-- prepare select? XXX
			body = "select merge_counter('" .. fields[1] .. "'::text, '" .. fields[2] .. "'::text, '" .. fields[3] .. "'::inet, '" .. fields[4] .. "'::smallint);"
		})
		if result.status ~= 200 or not result.body then
			ngx.log(ngx.ERR, 'postgres access failed')
			ngx.exit(500)
		end
	end

	red:init_pipeline(5)
	red:multi()
	red:del('stat_counter_pending_tmp')
	red:exists('stat_counter_pending')
	red:renamenx('stat_counter_pending', 'stat_counter_pending_tmp')
	red:exec()
	renamed, err = red:commit_pipeline()
	if not renamed then
		ngx.log(ngx.ERR, 'redis multi failed: ', err)
		ngx.exit(500)
	end

	if tonumber(renamed[5][1] == 0) or tonumber(renamed[5][2]) == 0 then
		continue = false
	end
end
