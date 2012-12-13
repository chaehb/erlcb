%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc view utility
%%
-module(erlcb_view).
-author('Chae,H B<chaehb@gmail.com>').

-include("erlcb.hrl").
-include("erlcb_http.hrl").

%% apis for query
-export([new_query/0,new_query/1,new_spatial_query/4]).

-export([query_value/2,is_spatial/1]).

-export([descending/1, descending/2,reduce/1,reduce/2,stale/1,stale/2]).
-export([key/1,key/2,keys/1,keys/2]).
-export([
	range/2,range/3,range/4,range/5,
	startkey/1,startkey/2,startkey_docid/1,startkey_docid/2,
	endkey/1,endkey/2,endkey_docid/1,endkey_docid/2,
	include_docs/1,include_docs/2,inclusive_end/1,inclusive_end/2]).

-export([group/1,group/2, group_level/1,group_level/2, limit/1,limit/2, skip/1,skip/2]).

-export([bbox/1, bbox/2,create_bbox/4]).

-export([on_error/2,full_set/2]).

-export([encode_query/1,encode_json/1]).

%% apis for response
-export([parse_response/1]).

-export([response/2]).
-export([row/2]).

%%===================================================================
%% API Function Definitions
%%===================================================================

-type view_query() :: #erlcb_view_query{} .

%% ------------------------------------------------------------------
%% view query manufacture
%% all query manufacturing functions has same response
%%
%% succcess : {ok, Query}
%% false : { error, Reason }
%% ------------------------------------------------------------------
%% @doc create normal view query
-spec new_query()
	-> { ok, Query :: view_query() } .
new_query()->
	{ok, #erlcb_view_query{}}.
	
%% @doc create spatial view query
-spec new_query(IsSpatial)
	-> {ok, Query :: view_query() } when IsSpatial :: boolean() .
new_query(IsSpatial) when is_boolean(IsSpatial) ->
	{ ok, #erlcb_view_query{spatial=IsSpatial} };
	
new_query(_)->
	{error,bad_args}.

%% @doc #erlcb_view_query -> QueryParams :: string() - Http 'get' Paramter 
encode_query(Query) 
		when is_record(Query,erlcb_view_query) ->
		
	%IsSpatial = Query#erlcb_view_query.spatial,

	Fields = record_info(fields,erlcb_view_query),
	[_|Values] = tuple_to_list(Query),
	ParamList=lists:filter(
		fun({Key,Value})->
			(Value =/= undefined) and (Key /= spatial)
		end,
		lists:zip(Fields,Values)
	),
	Params=lists:map(
		fun({Key,Value})->
			KeyString=list_to_binary(atom_to_list(Key)),
			%ValueString=binary_to_list(jiffy:encode(Value)),
			
			ValueBin=case is_binary(Value) of
				true ->
					Value;
				false ->
					case is_atom(Value) of
						true ->
							list_to_binary(atom_to_list(Value));
						false ->
							jiffy:encode(Value)
					end
			end,

			binary_to_list(<< KeyString/binary, <<"=">>/binary, ValueBin/binary >>)
		end,
		ParamList
	),
	QueryParams=string:join(Params,"&"),
	{ok, QueryParams};
	
encode_query(_) ->
	{error, bad_args}.
	
encode_json(Query)
		when is_record(Query,erlcb_view_query) ->

	%IsSpatial = Query#erlcb_view_query.spatial,

	Fields = record_info(fields,erlcb_view_query),
	[_|Values] = tuple_to_list(Query),
	ParamList=lists:filter(
		fun({Key,Value})->
			(Value =/= undefined) and (Key /= spatial)
		end,
		lists:zip(Fields,Values)
	),
	
	JsonQuery=jiffy:encode({ParamList}),
	
	{ok, JsonQuery};
	
encode_json(_)->
	{error, bad_args}.
	
%% @doc create spatial query all in all
-spec new_spatial_query(Bbox,Limit,Skip,Stale) 
	-> {ok, Query :: view_query() } when Bbox :: binary(), Limit :: integer(), Skip :: integer(), Stale :: stale().
new_spatial_query(Bbox,Limit,Skip,Stale)
		when is_binary(Bbox) and (is_integer(Limit) and is_integer(Skip)) ->
	case Stale of
		update_after ->
			{ ok, #erlcb_view_query{spatial=true,bbox=Bbox,limit=Limit,skip=Skip,stale=update_after}};
		false ->
			{ ok, #erlcb_view_query{spatial=true,bbox=Bbox,limit=Limit,skip=Skip,stale=false}};
		ok ->
			{ ok, #erlcb_view_query{spatial=true,bbox=Bbox,limit=Limit,skip=Skip,stale=ok}};
		_->
			{error, bad_args}
	end.
	
%% @doc get query value
query_value(Field,Query) 
		when is_record(Query,erlcb_view_query)  ->
	
	case Field of
		descending ->
			Query#erlcb_view_query.descending;
		endkey ->
			Query#erlcb_view_query.endkey;
		endkey_docid ->
			Query#erlcb_view_query.endkey_docid;
		full_set ->
			Query#erlcb_view_query.full_set;
		group ->
			Query#erlcb_view_query.group;
		group_level ->
			Query#erlcb_view_query.group_level;
		include_docs ->
			Query#erlcb_view_query.include_docs;
		inclusive_end ->
			Query#erlcb_view_query.inclusive_end;
		key ->
			Query#erlcb_view_query.key;
		keys ->
			Query#erlcb_view_query.keys;
		limit ->
			Query#erlcb_view_query.limit;
		on_error ->
			Query#erlcb_view_query.on_error;
		reduce ->
			Query#erlcb_view_query.reduce;
		skip ->
			Query#erlcb_view_query.skip;
		stale ->
			Query#erlcb_view_query.stale;
		startkey ->
			Query#erlcb_view_query.startkey;
		startkey_docid ->
			Query#erlcb_view_query.startkey_docid;
		bbox ->
			Query#erlcb_view_query.bbox;
		_->
			undefined
	end;

query_value(_,_) ->
	bad_args.
	
is_spatial(Query)
		when is_record(Query,erlcb_view_query) ->
	
	{ok, Query#erlcb_view_query.spatial};

is_spatial(_)->
	{error, bad_args}.

descending(IsDescending) 
		when is_boolean(IsDescending) ->
		
	{ok, #erlcb_view_query{descending=IsDescending}};
descending(_)->
	{error,bad_args}.
	
descending(Query,IsDescending) 
		when is_record(Query,erlcb_view_query) and is_boolean(IsDescending) ->
		
	{ok, Query#erlcb_view_query{descending=IsDescending}};
	
descending(_,_) ->
	{error, bad_args}.

reduce(IsReduce) 
		when is_boolean(IsReduce) ->
		
	{ok, #erlcb_view_query{reduce=IsReduce}};

reduce(_)->
	{error,bad_args}.

reduce(Query,IsReduce) 
		when is_record(Query,erlcb_view_query) and is_boolean(IsReduce) ->
		
	{ok, Query#erlcb_view_query{reduce=IsReduce}};

reduce(_,_)->
	{error,bad_args}.

stale(Stale) 
		when is_atom(Stale) ->
		
	case Stale of
		update_after ->
			{ok,#erlcb_view_query{stale=update_after}};
		false ->
			{ok,#erlcb_view_query{stale=false}};
		ok ->
			{ok,#erlcb_view_query{stale=ok}};
		_->
			{error, bad_args}
	end;
	
stale(_) ->
	{error,bad_args}.

stale(Query,Stale) 
		when is_record(Query,erlcb_view_query) and is_atom(Stale)->
		
	case Stale of
		update_after ->
			{ok,Query#erlcb_view_query{stale=update_after}};
		false ->
			{ok,Query#erlcb_view_query{stale=false}};
		ok ->
			{ok,Query#erlcb_view_query{stale=ok}};
		_->
			{error, bad_args}
	end;
	
stale(_,_) ->
	{error,bad_args}.

key(Key) 
		when is_binary(Key) ->
		
	{ok, #erlcb_view_query{key=Key} };

key(_) ->
	{error,bad_args}.

key(Query,Key) 
		when is_record(Query,erlcb_view_query) and is_binary(Key) ->
		
	{ok, Query#erlcb_view_query{key=Key} };

key(_,_) ->
	{error,bad_args}.

keys(Keys) 
		when is_binary(Keys) ->
		
	{ok, #erlcb_view_query{keys=Keys} };
	
keys(_)->
	{error,bad_args}.

keys(Query,Keys) 
		when is_record(Query,erlcb_view_query) and is_binary(Keys) ->
		
	{ok, Query#erlcb_view_query{keys=Keys} };
	
keys(_,_)->
	{error,bad_args}.

range(StartKey, EndKey) 
		when (is_binary(StartKey) and is_binary(EndKey)) ->

	{ok, #erlcb_view_query{startkey=StartKey,endkey=EndKey} };

range(_,_)->
	{error,bad_args}.

range(Query, StartKey, EndKey) 
		when is_record(Query,erlcb_view_query) and (is_binary(StartKey) and is_binary(EndKey)) ->

	{ok, Query#erlcb_view_query{startkey=StartKey,endkey=EndKey} };

range(_,_,_)->
	{error,bad_args}.

range(StartKey, EndKey, Skip, Limit) 
		when (is_binary(StartKey) and is_binary(EndKey)) and (is_integer(Skip) and is_integer(Limit)) ->

	{ok, #erlcb_view_query{startkey=StartKey,endkey=EndKey,skip=Skip,limit=Limit} };

range(_,_,_,_)->
	{error,bad_args}.

range(Query, StartKey, EndKey, Skip, Limit) 
		when (is_record(Query,erlcb_view_query) and (is_binary(StartKey) and is_binary(EndKey))) and (is_integer(Skip) and is_integer(Limit)) ->

	{ok, #erlcb_view_query{startkey=StartKey,endkey=EndKey,skip=Skip,limit=Limit} };

range(_,_,_,_,_)->
	{error,bad_args}.

startkey(StartKey)
		when is_binary(StartKey) ->
	
	{ok, #erlcb_view_query{startkey=StartKey} };

startkey(_)->
	{error,bad_args}.

startkey(Query,StartKey)
		when is_record(Query,erlcb_view_query) and is_binary(StartKey) ->
	
	{ok, Query#erlcb_view_query{startkey=StartKey} };

startkey(_,_)->
	{error,bad_args}.

endkey(EndKey)
		when is_binary(EndKey) ->
	
	{ok, #erlcb_view_query{endkey=EndKey} };

endkey(_)->
	{error,bad_args}.

endkey(Query,EndKey)
		when is_record(Query,erlcb_view_query) and is_binary(EndKey) ->
	
	{ok, Query#erlcb_view_query{endkey=EndKey} };

endkey(_,_)->
	{error,bad_args}.

startkey_docid(StartKeyDocId)
		when is_binary(StartKeyDocId) ->
	
	{ok, #erlcb_view_query{startkey_docid=StartKeyDocId} };

startkey_docid(_) ->
	{error,bad_args}.

startkey_docid(Query, StartKeyDocId)
		when is_record(Query,erlcb_view_query) and is_binary(StartKeyDocId) ->
	
	{ok, Query#erlcb_view_query{startkey_docid=StartKeyDocId} };

startkey_docid(_,_) ->
	{error,bad_args}.

endkey_docid(EndKeyDocId)
		when is_binary(EndKeyDocId) ->
	
	{ok, #erlcb_view_query{endkey_docid=EndKeyDocId} };

endkey_docid(_)->
	{error,bad_args}.

endkey_docid(Query, EndKeyDocId)
		when is_record(Query,erlcb_view_query) and is_binary(EndKeyDocId) ->
	
	{ok, Query#erlcb_view_query{endkey_docid=EndKeyDocId} };

endkey_docid(_,_)->
	{error,bad_args}.

include_docs(IncludeDocs)
		when is_boolean(IncludeDocs) ->
		
	{ok, #erlcb_view_query{include_docs=IncludeDocs}};

include_docs(_)->
	{error,bad_args}.
	
include_docs(Query, IncludeDocs)
		when is_record(Query,erlcb_view_query) and is_boolean(IncludeDocs) ->
		
	{ok, Query#erlcb_view_query{include_docs=IncludeDocs}};

include_docs(_,_)->
	{error,bad_args}.
	
inclusive_end(InclusiveEnd)
		when is_boolean(InclusiveEnd) ->
		
	{ok, #erlcb_view_query{inclusive_end=InclusiveEnd}};

inclusive_end(_)->
	{error,bad_args}.

inclusive_end(Query,InclusiveEnd)
		when is_record(Query,erlcb_view_query) and is_boolean(InclusiveEnd) ->
		
	{ok, Query#erlcb_view_query{inclusive_end=InclusiveEnd}};

inclusive_end(_,_)->
	{error,bad_args}.

group(Group)
		when is_boolean(Group) ->

	{ok, #erlcb_view_query{group=Group,reduce=true}};
		
group(_)->
	{error,bad_args}.
	
group(Query, Group)
		when is_record(Query,erlcb_view_query) and is_boolean(Group) ->
	case Query#erlcb_view_query.reduce of
		true ->
			{ok, Query#erlcb_view_query{group=Group}};
		undefined -> %% since undefine == true
			{ok, Query#erlcb_view_query{group=Group,reduce=true}};
		_ ->
			{error, not_reduce_query}
	end;

group(_,_)->
	{error,bad_args}.
	
group_level(GroupLevel) 
		when is_integer(GroupLevel) ->
	
	{ok, #erlcb_view_query{reduce=true, group_level=GroupLevel}};
	
group_level(_)->
	{error,bad_args}.

group_level(Query, GroupLevel) 
		when is_record(Query,erlcb_view_query) and is_integer(GroupLevel) ->
		
	case Query#erlcb_view_query.reduce of
		true ->
			{ok, Query#erlcb_view_query{group_level=GroupLevel}};
		undefined ->%% since undefine == true
			{ok, Query#erlcb_view_query{group_level=GroupLevel}};
		_ ->
			{error, not_reduce_query}
	end;
	
group_level(_,_)->
	{error,bad_args}.

limit(Limit)
		when is_integer(Limit) ->
		
	{ok, #erlcb_view_query{limit=Limit}};

limit(_)->
	{error,bad_args}.

limit(Query, Limit)
		when is_record(Query,erlcb_view_query) and is_integer(Limit) ->
		
	{ok, Query#erlcb_view_query{limit=Limit}};

limit(_,_)->
	{error,bad_args}.

skip(Skip)
		when is_integer(Skip) ->
		
	{ok, #erlcb_view_query{skip=Skip}};

skip(_)->
	{error,bad_args}.

skip(Query,Skip)
		when is_record(Query,erlcb_view_query) and is_integer(Skip) ->
		
	{ok, Query#erlcb_view_query{skip=Skip}};

skip(_,_)->
	{error,bad_args}.

on_error(Query,OnError)
		when is_record(Query,erlcb_view_query) ->
	case OnError of
		continue ->
			{ok, Query#erlcb_view_query{on_error=continue}};
		stop ->
			{ok, Query#erlcb_view_query{on_error=stop}};
		_->
			{error, bad_args}
	end;

on_error(_,_)->
	{error,bad_args}.
	
full_set(Query,FullSet) 
		when is_record(Query,erlcb_view_query) and is_boolean(FullSet) ->
		
	{ok, Query#erlcb_view_query{full_set=FullSet}};

full_set(_,_)->
	{error,bad_args}.

%% @doc set bbox for spatial view query
bbox(Bbox) when is_binary(Bbox) ->
	
	{ok,#erlcb_view_query{spatial=true,bbox=Bbox}};
	
bbox(_) ->
	{error,bad_args}.
	
-spec bbox(Query,Bbox)
	-> {ok, NewQuery :: view_query() } | {error,Reason :: atom()} when Query :: view_query(), Bbox :: binary() .

bbox(Query,Bbox) 
		when is_record(Query,erlcb_view_query) and is_binary(Bbox) ->
	case Query#erlcb_view_query.spatial of
		true ->
			case is_binary(Bbox) of
				true ->
					{ok,Query#erlcb_view_query{bbox=Bbox}};
				false->
					{error,bad_args}
			end;
		false ->
			{error,not_spatial_query}
	end;
bbox(Query,{LowerLeftX,LowerLeftY,UpperRightX,UpperRightY}) 
		when is_record(Query,erlcb_view_query) ->
		
	case Query#erlcb_view_query.spatial of
		true ->
			case create_bbox(LowerLeftX,LowerLeftY,UpperRightX,UpperRightY) of
				{ok,Bbox} ->
					{ok, Query#erlcb_view_query{bbox=Bbox}};
				{error,Reason}->
					{error,Reason}
			end;
		false ->
			{error,not_spatial_query}
	end;
	
bbox(_,_)->
	{error, bad_args}.

create_bbox(LowerLeftX,LowerLeftY,UpperRightX,UpperRightY) 
		when (is_integer(LowerLeftX) or is_float(LowerLeftX))
			and (is_integer(LowerLeftY) or is_float(LowerLeftY)) 
			and (is_integer(UpperRightX) or is_float(UpperRightX)) 
			and (is_integer(UpperRightY) or is_float(UpperRightY)) ->
	
	{ok,list_to_binary(lists:flatten(io_lib:fwrite("~.1f,~.1f,~.1f,~.1f",[LowerLeftX*1.0, LowerLeftY*1.0, UpperRightX*1.0, UpperRightY*1.0])))};	
	
create_bbox(_,_,_,_) ->
	{error, bad_args}.
%% ------------------------------------------------------------------
%% view response managements
%% ------------------------------------------------------------------

parse_response({Status,Response})->
	case Status of
		?OK ->
			{Res}=jiffy:decode(Response),
			{Status,encode_view_response(Res)};
		_->
			Error=jiffy:decode(Response),
			{Status,#erlcb_view_response{status=Status,error=Error}}
	end.


response(status,Res)->
	Res#erlcb_view_response.status ;

response(error, Res) ->
	Res#erlcb_view_response.error ;
	
response(total_rows, Res) ->
	Res#erlcb_view_response.total_rows ;

response(update_seq, Res)->
	Res#erlcb_view_response.update_seq ;

response(reduce, Res)->
	Res#erlcb_view_response.reduce ;

response(ids, Res)->
	case Res#erlcb_view_response.reduce of
		false ->
			Rows=Res#erlcb_view_response.rows,
			lists:map(
				fun(Row)->
					Row#erlcb_view_row.id
				end,
				Rows
			);
		true ->
			undefined
	end;

response(row_count, Res)->
	case Res#erlcb_view_response.rows of
		undefined ->
			0;
		Rows ->
			length(Rows)
	end;

response(rows, Res)->
	Res#erlcb_view_response.rows .

row(id, Row)->
	Row#erlcb_view_row.id ;

row(key, Row)->
	Row#erlcb_view_row.key ;
	
row(bbox, Row)->
	Row#erlcb_view_row.bbox ;
	
row(geometry, Row)->
	Row#erlcb_view_row.geometry ;
	
row(value, Row)->
	Row#erlcb_view_row.value .

%%===================================================================
%% Internal Function Definitions
%%===================================================================
%% spatial response : total_rows change update_seq
encode_view_response(Res) -> 
case proplists:is_defined(<<"update_seq">>,Res) of
	true ->
		encode_view_response(spatial,Res);
	false ->
		encode_view_response(proplists:is_defined(<<"total_rows">>,Res),Res)
end.


encode_view_response(true,Res) ->
	TotalRows=proplists:get_value(<<"total_rows">>,Res),

	RowSources=proplists:get_value(<<"rows">>,Res),
	Rows=lists:map(
		fun({RowSource})->
			Value=case proplists:get_value(<<"value">>,RowSource) of
				V1 when is_tuple(V1)->
					{V2}=V1,
					V2;
				V1 ->
					V1
			end,
			#erlcb_view_row{
				id=proplists:get_value(<<"id">>,RowSource),
				key=proplists:get_value(<<"key">>,RowSource),
 	 			value=Value
			}
		end,
		RowSources
	),
			
	#erlcb_view_response{
		status=?OK,
		reduce=false,
		total_rows=TotalRows,
		rows=Rows
	};

encode_view_response(spatial,Res) ->
	RowSources=proplists:get_value(<<"rows">>,Res),
	Rows=lists:map(
		fun({RowSource})->
			Value=case proplists:get_value(<<"value">>,RowSource) of
				V1 when is_tuple(V1)->
					{V2}=V1,
					V2;
				V1 ->
					V1
			end,
			{Geometry}=proplists:get_value(<<"geometry">>,RowSource),
			#erlcb_view_row{
				key=proplists:get_value(<<"key">>,RowSource),
				bbox=proplists:get_value(<<"bbox">>,RowSource),
				geometry=Geometry,
				value=Value
			}
		end,
		RowSources
	),
	
	#erlcb_view_response{
		status=?OK,
		update_seq=proplists:get_value(<<"update_seq">>,Res),
		rows=Rows
	};

encode_view_response(false,Res)-> % when reduce = true
	RowSources=proplists:get_value(<<"rows">>,Res),
	Rows=lists:map(
		fun({RowSource})->
			Value=case proplists:get_value(<<"value">>,RowSource) of
				V1 when is_tuple(V1)->
					{V2}=V1,
					V2;
				V1 ->
					V1
			end,
			#erlcb_view_row{
				key=proplists:get_value(<<"key">>,RowSource),
				value=Value
			}
		end,
		RowSources
	),
	#erlcb_view_response{
		status=?OK,
		reduce=true,
		rows=Rows
	}.
