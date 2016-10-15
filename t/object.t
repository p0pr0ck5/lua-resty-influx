use Test::Nginx::Socket::Lua;

plan tests => 3 * blocks() - 1;

no_shuffle();
run_tests();

__DATA__

=== TEST 1: Create a new object with basic options
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local opts = {
			}

			local influx, err = iobj:new(opts)

			ngx.say(influx.host)
			ngx.say(influx.port)
			ngx.say(influx.proto)
			ngx.say(influx.precision)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
127.0.0.1
8086
http
ms
nil
--- no_error_log
[error]

=== TEST 2: Create a new object, defining each option
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local opts = {
				host = "example.com",
				port = 12345,
				db = "testdb",
				hostname = "hostname.tld",
				proto = "udp",
				precision = "s"
			}

			local influx, err = iobj:new(opts)

			ngx.say(influx.host)
			ngx.say(influx.port)
			ngx.say(influx.db)
			ngx.say(influx.hostname)
			ngx.say(influx.proto)
			ngx.say(influx.precision)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
example.com
12345
testdb
hostname.tld
udp
s
nil
--- no_error_log
[error]

=== TEST 3: Invalid host
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local opts = {
				host = true,
				db = "testdb"
			}

			local influx, err = iobj:new(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid host
--- no_error_log
[error]

=== TEST 4: Invalid port
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local opts = {
				port = true,
				db = "testdb"
			}

			local influx, err = iobj:new(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid port
--- no_error_log
[error]

=== TEST 5: Invalid db
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local opts = {
				db = 1
			}

			local influx, err = iobj:new(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid db
--- no_error_log
[error]

=== TEST 6: Invalid hostname
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local opts = {
				hostname = true
			}

			local influx, err = iobj:new(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid hostname
--- no_error_log
[error]

=== TEST 7: Invalid proto type
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local opts = {
				proto = 1
			}

			local influx, err = iobj:new(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid proto 1
--- no_error_log
[error]

=== TEST 8: Invalid proto value
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local opts = {
				proto = "foo"
			}

			local influx, err = iobj:new(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid proto foo
--- no_error_log
[error]

=== TEST 9: Invalid precision
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local opts = {
				precision = true
			}

			local influx, err = iobj:new(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid precision
--- no_error_log
[error]

=== TEST 10: Invalid opts type
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local opts = true

			local influx, err = iobj:new(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
opts must be a table
--- no_error_log
[error]

=== TEST 11: write_udp success
--- config
	location /t {
		content_by_lua '
			ngx.socket.udp = function() return {
				setpeername = function(host, port) return true end,
				send = function(data) return true end,
			} end

			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			local ok, err = influx:write_udp("foo bar")

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
true
nil
--- no_error_log
[error]

=== TEST 12: write_udp fails on setpeername
--- config
	location /t {
		content_by_lua '
			ngx.socket.udp = function() return {
				setpeername = function(host, port) return false, "setpeername failure" end,
				send = function(data) return true end,
			} end

			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			local ok, err = influx:write_udp("foo bar")

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
false
setpeername failure
--- no_error_log
[error]

=== TEST 13: write_udp fails on send
--- config
	location /t {
		content_by_lua '
			ngx.socket.udp = function() return {
				setpeername = function(host, port) return true end,
				send = function(data) return false, "send failure" end,
			} end

			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			local ok, err = influx:write_udp("foo bar")

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
false
send failure
--- no_error_log
[error]

=== TEST 14: do_write returns proto write success (1/2)
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({
				proto = "http"
			})

			influx.write_http = function(self, msg) return true end

			local ok, err = influx:do_write("foo bar")
			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
true
nil
--- no_error_log
[error]

=== TEST 15: do_write returns proto write success (2/2)
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({
				proto = "udp"
			})

			influx.write_udp = function(self, msg) return true end

			local ok, err = influx:do_write("foo bar")
			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
true
nil
--- no_error_log
[error]

=== TEST 16: do_write fails when proto is invalid
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx.proto = "foo"

			local ok, err = influx:do_write("foo bar")
			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
false
unknown proto
--- no_error_log
[error]

=== TEST 17: add tag updates object tag count
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			ngx.say(influx._tag_cnt)

			influx:add_tag("foo", "bar")

			ngx.say(influx._tag_cnt)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
0
1
--- no_error_log
[error]

=== TEST 18: add tag formats key and value
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx:add_tag("foo", "bar")

			ngx.say(influx._tag_set[1])
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
foo=bar
--- no_error_log
[error]

=== TEST 19: add field updates object tag count
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			ngx.say(influx._field_cnt)

			influx:add_field("foo", "bar")

			ngx.say(influx._field_cnt)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
0
1
--- no_error_log
[error]

=== TEST 20: add field formats key and value
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx:add_field("foo", "bar")

			ngx.say(influx._field_set[1])
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
foo="bar"
--- no_error_log
[error]

=== TEST 21: set_measurement sets the measurement
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx:set_measurement("foo")

			ngx.say(influx._measurement)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
foo
--- no_error_log
[error]

=== TEST 22: stamp sets object value when called
--- config
	location /t {
		content_by_lua '
			-- stamp() by default calls ngx.now() * 1000
			ngx.now = function() return 123 end

			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx:stamp()

			ngx.say(influx._stamp)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
123000
--- no_error_log
[error]

=== TEST 23: stamp sets object value with second precision
--- config
	location /t {
		content_by_lua '
			-- stamp() by default calls ngx.time() with second precision
			ngx.time = function() return 456 end

			local iobj = require "resty.influx.object"

			local influx = iobj:new({
				precision = "s"
			})

			influx:stamp()

			ngx.say(influx._stamp)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
456
--- no_error_log
[error]

=== TEST 24: stamp sets an empty string when precision is otherwise
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({
				precision = "none"
			})

			influx:stamp()

			ngx.say(influx._stamp .. "x")
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
x
--- no_error_log
[error]

=== TEST 25: stamp sets _stamp with a given param
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({
				precision = "none"
			})

			influx:stamp(12345)

			ngx.say(influx._stamp)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
12345
--- no_error_log
[error]

=== TEST 26: stamp logs an error when an invalid param is passed
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({
				precision = "none"
			})

			influx:stamp(true)

			ngx.say(influx._stamp)
		';
	}
--- request
GET /t
--- error_code: 200
--- error_log
invalid stamp type

=== TEST 27: timestamp returns _stamp
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx._stamp = 12345

			ngx.say(influx:timestamp())
		';
	}
--- request
GET /t
--- response_body
12345
--- error_code: 200
--- no_error_log
[error]

=== TEST 28: timestamp calls stamp when _stamp is unset
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx.stamp = function(self, time) self._stamp = 12345 end

			ngx.say(influx:timestamp())
		';
	}
--- request
GET /t
--- response_body
12345
--- error_code: 200
--- no_error_log
[error]

=== TEST 29: string representation of object
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx._measurement = "foo"
			influx._stamp = 12345
			influx._tag_cnt = 1
			influx._tag_set = { "foo=bar" }
			influx._field_cnt = 1
			influx._field_set = { "foo=bar" }

			ngx.say(tostring(influx))
		';
	}
--- request
GET /t
--- response_body
foo,12345,1,foo=bar,1,foo=bar
--- error_code: 200
--- no_error_log
[error]

=== TEST 30: clear resets obj fields
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx._measurement = "foo"
			influx._stamp = 12345
			influx._tag_cnt = 1
			influx._tag_set = { "foo=bar" }
			influx._field_cnt = 1
			influx._field_set = { "foo=bar" }

			influx:clear()

			ngx.say(tostring(influx))
		';
	}
--- request
GET /t
--- response_body
nil,nil,0,,0,
--- error_code: 200
--- no_error_log
[error]

=== TEST 31: buffer_ready returns true when obj is ready
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx:set_measurement("foo")
			influx:add_field("value", 1)

			local ok, err = influx:buffer_ready()

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- response_body
true
nil
--- error_code: 200
--- no_error_log
[error]

=== TEST 32: buffer_ready returns false when _measurement is unset
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx:add_field("value", 1)

			local ok, err = influx:buffer_ready()

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- response_body
false
no measurement
--- error_code: 200
--- no_error_log
[error]

=== TEST 33: buffer_ready returns false when _field_cnt is 0
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx:set_measurement("foo")

			local ok, err = influx:buffer_ready()

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- response_body
false
no fields
--- error_code: 200
--- no_error_log
[error]

=== TEST 34: flush_ready returns true when obj is ready
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx._msg_cnt = 1

			local ok, err = influx:flush_ready()

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- response_body
true
nil
--- error_code: 200
--- no_error_log
[error]

=== TEST 35: flush_ready returns false when _measurement is set
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx._measurement = "foo"

			local ok, err = influx:flush_ready()

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- response_body
false
unbuffered measurement
--- error_code: 200
--- no_error_log
[error]

=== TEST 36: flush_ready returns false when _field_cnt is not 0
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx._field_cnt = 1

			local ok, err = influx:flush_ready()

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- response_body
false
unbuffered fields
--- error_code: 200
--- no_error_log
[error]

=== TEST 37: flush_ready returns false when _msg_cnt is 0
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			local ok, err = influx:flush_ready()

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- response_body
false
no buffered fields
--- error_code: 200
--- no_error_log
[error]

=== TEST 38: buffer returns influx:clear() when valid and sets appropriate fields
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx:set_measurement("foo")
			influx:add_field("value", 1)

			local ok, err = influx:buffer()

			ngx.say(ok)
			ngx.say(err)
			ngx.say(influx._msg_cnt)
		';
	}
--- request
GET /t
--- response_body
true
nil
1
--- error_code: 200
--- no_error_log
[error]

=== TEST 39: buffer fails when not ready
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx:set_measurement("foo")
			influx:add_field("value", 1)

			influx.buffer_ready = function() return false, "buffer not ready" end

			local ok, err = influx:buffer()

			ngx.say(ok)
			ngx.say(err)
			ngx.say(influx._msg_cnt)
		';
	}
--- request
GET /t
--- response_body
false
buffer not ready
0
--- error_code: 200
--- no_error_log
[error]

=== TEST 40: flush returns do_write
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx.do_write = function() return true end

			influx:set_measurement("foo")
			influx:add_field("value", 1)
			influx:buffer()

			local ok, err = influx:flush()

			ngx.say(ok)
			ngx.say(err)
			ngx.say(influx._msg_cnt)
		';
	}
--- request
GET /t
--- response_body
true
nil
0
--- error_code: 200
--- no_error_log
[error]

=== TEST 41: flush fails when flush_ready fails
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx:set_measurement("foo")
			influx:add_field("value", 1)
			influx:buffer()

			influx.flush_ready = function() return false, "flush not ready" end

			local ok, err = influx:flush()

			ngx.say(ok)
			ngx.say(err)
			ngx.say(influx._msg_cnt)
		';
	}
--- request
GET /t
--- response_body
false
flush not ready
1
--- error_code: 200
--- no_error_log
[error]

=== TEST 42: write succeeds and returns clear()
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx.do_write = function() return true end

			influx:set_measurement("foo")
			influx:add_field("value", 1)

			local ok, err = influx:write()

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- response_body
true
nil
--- error_code: 200
--- no_error_log
[error]

=== TEST 43: write fails when flush_ready fails
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx.do_write = function() return true end
			influx.buffer_ready = function() return false, "buffer not ready" end

			influx:set_measurement("foo")
			influx:add_field("value", 1)

			local ok, err = influx:write()

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- response_body
false
buffer not ready
--- error_code: 200
--- no_error_log
[error]

=== TEST 44: write fails when do_write fails
--- config
	location /t {
		content_by_lua '
			local iobj = require "resty.influx.object"

			local influx = iobj:new({})

			influx.do_write = function() return false, "do_write failed" end

			influx:set_measurement("foo")
			influx:add_field("value", 1)

			local ok, err = influx:write()

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- response_body
false
do_write failed
--- error_code: 200
--- no_error_log
[error]

