%% @doc couchbase server configuration

-record(erlcb_node,{
	index :: atom(),
	protocol = http :: 'http'| 'https',
	host="localhost" :: string(), 
	port=8091 :: integer(), %% default 8091
	membership :: string(), %% "active" is normal
	status :: string(), %% "healthy" is normal
	couchApi :: list(), %% [ full base view url '...'/_design]
	direct :: integer(), %% port
	proxy :: integer() %% 
}).

-record(erlcb_couchbase,{
	name :: atom(), % unique name when using supervisor registration
	protocol = http :: 'http' | 'https',
	host = "localhost" :: string(),
	port = 8091 :: integer(), %% 8091
	bucket = ["default",""] :: list( string() ),
	view =development :: 'development' | 'production',
	streamingUri :: string(), %% streaming url
	checksum = -1 :: integer(),
	node=[] :: list(#erlcb_node{})
}).

%% @doc stale
%% update_after : Allow stale view, update view after it has been accessed
%% false : Force a view update before returning data
%% ok : Allow stale views
-type stale() :: 'update_after' | 'false' | 'ok' .

%% @doc on_error
%% continue : Continue to generate view information in the event of an error, 
%% 		including the error information in the view response stream.
%% stop : Stop immediately when an error condition occurs. 
%%		No further view information will be returned.
-type on_error() :: 'continue' | 'stop' .

%% @doc view query
-record(erlcb_view_query,{
	descending :: boolean(), %% undefined => couchbase default => false
	endkey :: binary(),
	endkey_docid :: binary(),
	full_set ::  boolean(), %% undefined => couchbase default => false,  development views only
	group ::  boolean(),%% undefined => couchbase default => false
	group_level :: integer(),
	include_docs ::  boolean(),%% undefined => couchbase default => maybe false
	inclusive_end ::  boolean(),%% undefined => couchbase default => true
	key :: binary(),
	keys :: list(binary()),
	limit :: integer(),
	on_error :: on_error() , %% undefined => couchbase default => continue
	reduce ::  boolean(),%% undefined => couchbase default => true
	skip :: integer(),%% undefined => couchbase default => 0
	stale :: stale(),%% undefined => couchbase default => update_after
	startkey :: binary(),
	startkey_docid :: binary(),

	bbox :: binary(), %% four comma separated numbers .1f or integer e.g. <<"0,0,180.0,90">> : from GeoJSON lower left, upper right
	spatial = false ::  boolean() %% not couchbase view argument; only for erlcb
}).


%% @doc view response
-record(erlcb_view_row,{
	id :: binary(),
	key :: term(),
	bbox :: term(),
	geometry :: term(),
	value :: term()
}).

-record(erlcb_view_response,{
	status :: integer(),
	error :: term() ,
	reduce ::  boolean(),
	total_rows :: integer(),
	update_seq :: integer(),
	rows :: list(#erlcb_view_row{})
}).
