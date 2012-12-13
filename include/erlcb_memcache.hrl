%% response status codes
-define(CB_NOT_CONNECTED,-1).
-define(CB_RECV_ERROR,-2).
-define(CB_UNEXPECTED_VBUCKET_ERROR,-3).
-define(CB_UNIMPLEMENTED,-4).
-define(CB_UNKNOWN_COMMAND, -5).
-define(CB_UNEXPECTED_ERROR, -6).
-define(CB_BAD_REQUEST, -7).
-define(CB_CONNECTION_NOT_FOUND, -8).

%% @doc state using erlcb_memcache
-record(erlcb_memcache,{
	host :: string(),
	port :: integer(),
	bucket :: string(),
	password :: string(),
	connected :: 'true' | 'false',
	socket :: gen_tcp:socket()
}).

%% @doc memcached protocol packet defintions
-record(mc_header,{
	magic = 0,
	opcode = 0,
	keylen = 0,
	extraslen=0,
	datatype = 0,
	reserve = 0,% request -> vbucket id, response -> status
	bodylen=0,
	opaque=0,
	cas = 0
}).

-record(mc_body,{
	extras= <<>>,
	key = <<>>,
	data= <<>>
}).
