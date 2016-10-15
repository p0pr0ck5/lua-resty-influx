local _M = {}

local lp   = require "resty.influx.lineproto"
local http = require "resty.http"

local log = ngx.log
local udp = ngx.socket.udp

local str_gsub = string.gsub
local str_rep  = string.rep
local str_sub  = string.sub
local str_find = string.find
local str_fmt  = string.format
local tbl_cat  = table.concat
local floor    = math.floor
local timer_at = ngx.timer.at

local ERR = ngx.ERR
local WRN = ngx.WARN
local DBG = ngx.DEBUG

_M.version = "0.1"

local mt = {
		__index = _M,
		__tostring = function(self)
			return str_fmt(
				"%s,%s,%s,%s,%s,%s",
				tostring(self._measurement),
				tostring(self._stamp),
				tostring(self._tag_cnt),
				tostring(tbl_cat(self._tag_set, '|')),
				tostring(self._field_cnt),
				tostring(tbl_cat(self._field_set, '|'))
			) end
}

function _M.write_udp(self, msg)
	local sock = udp()

	local ok, err_msg

	ok, err_msg = sock:setpeername(self.host, self.port)

	if (not ok) then
		return false, err_msg
	end

	ok, err_msg = sock:send(msg)

	if (not ok) then
		return false, err_msg
	end

	return true
end

function _M.write_http(self, msg)
	local httpc = http.new()

	httpc:set_timeout(1000)
	local ok, err_msg = httpc:connect(self.host, self.port)

	if (not ok) then
		return false, err_msg
	end

	local query_str = str_fmt("/write?db=%s&precision=%s", self.db, self.precision)

	local res, err_msg = httpc:request({
		path   = query_str,
		method = 'POST',
		headers = {
			["Host"] = self.hostname or self.host
		},
		body = msg,
	})

	if (res.status == 204) then
		return true
	elseif (res) then
		-- assume that for now influx didnt send us more than 8k in body res
		local body = res.body_reader(8192)

		return false, body
	else
		return false, err_msg
	end
end

function _M.do_write(self, msg)
	local proto = self.proto

	if (proto == 'http') then
		return self:write_http(msg)
	elseif (proto == 'udp') then
		return self:write_udp(msg)
	else
		return false, 'unknown proto'
	end
end

function _M.add_tag(self, key, value)
	local tag_cnt = self._tag_cnt + 1

	key   = lp.quote_tag_part(key)
	value = lp.quote_tag_part(value)

	self._tag_cnt = tag_cnt

	-- TODO sort tags by keys
	self._tag_set[tag_cnt] = str_fmt("%s=%s", key, value)
end

function _M.add_field(self, key, value)
	local field_cnt = self._field_cnt + 1

	key = lp.quote_field_key(key)

	if (type(value) == 'string') then
		value = lp.quote_field_value(value)
	end

	self._field_cnt = field_cnt

	self._field_set[field_cnt] = str_fmt("%s=%s", key, value)
end

function _M.set_measurement(self, measurement)
	self._measurement = lp.quote_measurement(measurement)
end

function _M.stamp(self, time)
	if (time) then
		if (type(time) == 'number') then
			self._stamp = time
			return
		else
			log(ERR, "invalid stamp type")
		end
	end

	local precision = self.precision

	if (precision == 'ms') then
		self._stamp = tostring(ngx.now() * 1000)
	elseif (precision == 's') then
		self._stamp = tostring(ngx.time())
	else
		self._stamp = ''
	end
end

function _M.timestamp(self)
	local stamp = self._stamp

	if (not stamp) then
		self:stamp()
		return self._stamp
	else
		return stamp
	end
end

function _M.clear(self)
	self._measurement = nil
	self._stamp = nil
	self._tag_cnt = 0
	self._tag_set = {}
	self._field_cnt = 0
	self._field_set = {}

	return true
end

function _M.buffer_ready(self)
	if (not self._measurement) then
		return false, 'no measurement'
	end

	if (self._field_cnt == 0) then
		return false, 'no fields'
	end

	return true
end

function _M.flush_ready(self)
	if (self._measurement) then
		return false, 'unbuffered measurement'
	end

	if (self._field_cnt ~= 0) then
		return false, 'unbuffered fields'
	end

	if (self._msg_cnt == 0) then
		return false, 'no buffered fields'
	end

	return true
end

function _M.buffer(self)
	local ready, err_msg = self:buffer_ready()

	if (not ready) then
		return false, err_msg
	end

	self:timestamp()

	local msg = lp.build_line_proto_stmt(self)

	local msg_cnt = self._msg_cnt + 1
	self._msg_cnt = msg_cnt
	self._msg_buf[msg_cnt] = msg

	-- clear entries for another elt
	return self:clear()
end

function _M.flush(self)
	local ready, err_msg = self:flush_ready()

	if (not ready) then
		return false, err_msg
	end

	local msg = tbl_cat(self._msg_buf, "\n")

	self._msg_cnt = 0
	self._msg_buf = {}

	return self:do_write(msg)
end

function _M.write(self)
	local ready, err_msg = self:buffer_ready()

	if (not ready) then
		return false, err_msg
	end

	self:timestamp()

	local ok, err_msg = self:do_write(lp.build_line_proto_stmt(self))

	if (not ok) then
		return false, err_msg
	end

	return self:clear()
end

function _M.new(self, opts)
	if (type(opts) ~= 'table') then
		return false, 'opts must be a table'
	end

	local host      = opts.host or '127.0.0.1'
	local port      = opts.port or 8086
	local db        = opts.db or ''
	local hostname  = opts.hostname or host
	local proto     = opts.proto or 'http'
	local precision = opts.precision or 'ms'

	local t = {
		-- user opts
		host      = host,
		port      = port,
		db        = db,
		hostname  = hostname,
		proto     = proto,
		precision = precision,

		-- obj fields
		_tag_cnt   = 0,
		_tag_set   = {},
		_field_cnt = 0,
		_field_set = {},
		_msg_cnt   = 0,
		_msg_buf   = {},
	}

	if (type(host) ~= 'string') then
		return nil, 'invalid host'
	end
	if (type(port) ~= 'number' or port < 0 or port > 65535) then
		return nil, 'invalid port'
	end
	if (type(db) ~= 'string') then
		return nil, 'invalid db'
	end
	if (type(hostname) ~= 'string') then
		return nil, 'invalid hostname'
	end
	if (type(proto) ~= 'string' or (proto ~= 'http' and proto ~= 'udp')) then
		return nil, 'invalid proto ' .. tostring(proto)
	end
	if (type(precision) ~= 'string') then
		return nil, 'invalid precision'
	end

	return setmetatable(t, mt)
end

return _M
