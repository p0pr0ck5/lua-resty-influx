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

local host
local port
local db
local precision
local proto
local hostname
local initted = false

local msg_cnt = 0
local msg_buf = {}

_M.version = "0.1"

local function _write_udp(msg)
	local sock = udp()

	local ok, err_msg

	ok, err_msg = sock:setpeername(host, port)

	if (not ok) then
		return false, err_msg
	end

	ok, err_msg = sock:send(msg)

	if (not ok) then
		return false, err_msg
	end

	return true
end

local function _write_http(msg)
	local httpc = http.new()

	httpc:set_timeout(1000)
	local ok, err_msg = httpc:connect(host, port)

	if (not ok) then
		return false, err_msg
	end

	local query_str = str_fmt("/write?db=%s&precision=%s", db, precision)

	local res, err_msg = httpc:request({
		path   = query_str,
		method = 'POST',
		headers = {
			["Host"] = hostname
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

function _do_write(p, msg)
	ngx.log(ngx.DEBUG, msg)
	ngx.log(ngx.DEBUG, proto)

	if (proto == 'http') then
		return _write_http(msg)
	elseif (proto == 'udp') then
		return _write_udp(msg)
	else
		return false, 'unknown proto'
	end
end

function _M.clear()
	msg_cnt = 0
	msg_buf = {}

	return true
end

function _M.buffer(data)
	local influx_data = {
		_measurement = lp.quote_measurement(data.measurement),
		_tag_set = lp.build_tag_set(data.tags),
		_field_set = lp.build_field_set(data.fields),
		_stamp = ngx.now() * 1000
	}

	local msg = lp.build_line_proto_stmt(influx_data)

	msg_cnt = msg_cnt + 1
	msg_buf[msg_cnt] = msg

	return true
end

function _M.flush()
	local msg = tbl_cat(msg_buf, "\n")
	_M.clear()

	return timer_at(0, _do_write, msg)
end

function _M.init(opts)
	if (initted) then
		return false, 'already initted'
	end

	local _host      = opts.host
	local _port      = opts.port
	local _db        = opts.db
	local _hostname  = opts.hostname or host
	local _proto     = opts.proto or 'http'
	local _precision = opts.precision or 'ms'

	if (type(_host) ~= 'string') then
		return nil, 'invalid host ' .. type(_host)
	end
	if (type(_port) ~= 'number' or _port < 0 or _port > 65535) then
		return nil, 'invalid port'
	end
	if (type(_db) ~= 'string') then
		return nil, 'invalid db'
	end
	if (type(_proto) ~= 'string' or (_proto ~= 'http' and _proto ~= 'udp')) then
		return nil, 'invalid proto ' .. _proto
	end
	if (type(_precision) ~= 'string') then
		return nil, 'invalid precision'
	end

	host      = _host
	port      = _port
	db        = _db
	hostname  = _hostname
	proto     = _proto
	precision = _precision
	initted   = true

	return true
end

return _M
