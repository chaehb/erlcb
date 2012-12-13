%% @doc couchapi query and response definitions

-record(viw_query,{
	spatial = false :: 'true' | 'false' ,
	descending :: boolean(),
	endkey :: binary() | list(binary()),
	endkey_docid :: binary(),
	full_set :: boolean(), % development views only
	group :: boolean(),
	group_level :: integer(),
	inclusive_end :: boolean(),
	key :: binary(),
	keys :: list(binary()),
	limit :: integer(),
	on_error :: 'continue' | 'stop' , 
	reduce :: boolean(),
	skip :: integer(),
	stale :: 'false' | 'ok' | 'update_after',
	startkey :: binary(),
	startkey_docid :: binary(),
	bbox :: binary()
}).

-record(view_row,{
	id :: binary(),
	key :: binary(),
	value :: list(binary())
}).

-record(view_response,{
	status :: integer(),
	error :: term() ,
	reduce :: 'true' | 'false',
	total_rows :: integer(),
	rows :: list(#view_row{})
}).
