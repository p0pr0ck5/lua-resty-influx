local _M = {}

local http = require "resty.http"

local encode_base64 = ngx.encode_base64
local log = ngx.log
local udp = ngx.socket.udp

local str_fmt  = string.format

local HTTP_NO_CONTENT = ngx.HTTP_NO_CONTENT

_M.version = "0.2"

function _M.write_udp(msg, host, port)
	local sock = udp()

	local ok, err

	ok, err = sock:setpeername(host, port)
	if not ok then
		return false, err
	end

	ok, err = sock:send(msg)
	if not ok then
		return false, err
	end

	return true, nil
end

function _M.write_http(msg, params)
	local client = http.new()

	local scheme     = 'http'
	local ssl_verify = false

	if params.ssl then
		scheme     = 'https'
		ssl_verify = true
	end

	local path    = str_fmt('%s://%s:%s/write', scheme, params.host, params.port)
	local method  = 'POST'
	local headers = {
		["Host"]  = params.hostname
	}

	if params.auth then
		headers.Authorization = str_fmt("Basic %s", encode_base64(params.auth))
	end

	local res, err = client:request_uri(
		path,
		{
			query      = { db = params.db, precision = params.precision },
			method     = method,
			headers    = headers,
			body       = msg,
			ssl_verify = ssl_verify,
		}
	)

	if not res then
		return false, err
	end

	if res.status == HTTP_NO_CONTENT then
		return true
	else
		return false, res.body
	end
end

function _M.validate_options(opts)
	if type(opts) ~= 'table' then
		return false, 'opts must be a table'
	end

	opts.host      = opts.host or '127.0.0.1'
	opts.port      = opts.port or 8086
	opts.db        = opts.db or 'lua-resty-influx'
	opts.hostname  = opts.hostname or opts.host
	opts.proto     = opts.proto or 'http'
	opts.precision = opts.precision or 'ms'
	opts.ssl       = opts.ssl or false
	opts.auth      = opts.auth or nil

	if type(opts.host) ~= 'string' then
		return false, 'invalid host'
	end
	if type(opts.port) ~= 'number' or opts.port < 0 or opts.port > 65535 then
		return false, 'invalid port'
	end
	if type(opts.db) ~= 'string' or opts.db == '' then
		return false, 'invalid db'
	end
	if type(opts.hostname) ~= 'string' then
		return false, 'invalid hostname'
	end
	if type(opts.proto) ~= 'string' or (opts.proto ~= 'http' and opts.proto ~= 'udp') then
		return false, 'invalid proto ' .. tostring(opts.proto)
	end
	if type(opts.precision) ~= 'string' then
		return false, 'invalid precision'
	end
	if type(opts.ssl) ~= 'boolean' then
		return false, 'invalid ssl'
	end
	if opts.auth and type(opts.auth) ~= 'string' then
		return false, 'invalid auth'
	end
	return true
end

return _M
