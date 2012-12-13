%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc client interface module for this couchbase connection
%%
-module(erlcb_couchbase).

-author('Chae,H B<chaehb@gmail.com>').

-behaviour(gen_server).

-include("erlcb.hrl").
-include("erlcb_memcache.hrl").
-include("mc_constants.hrl").
-include("erlcb_http.hrl").

-record(erlcb_client_state,{name,bucket_table,couchapi,mc_direct,mc_proxy,round_robin=0}).
%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(CouchbaseName) ->
    gen_server:start_link({local,CouchbaseName},?MODULE, CouchbaseName, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(CouchbaseName) ->
	process_flag(trap_exit, true),
	
	Name=atom_to_list(CouchbaseName),
	BucketTable=erlcb_util:append_to_atom(CouchbaseName,'_vbucket'),
	CouchApi=Name++"_couchapi_",
	McDirect=Name++"_mc_direct_",
	McProxy=Name++"_mc_proxy_",
    
    {ok, #erlcb_client_state{name=CouchbaseName,bucket_table=BucketTable,couchapi=CouchApi,mc_direct=McDirect,mc_proxy=McProxy}}.
    
handle_call({multi_get,{Keys}},_From,State)->
	try
		Requests=lists:map(
			fun(Key)->
				BinKey=case is_binary(Key) or is_list(Key) of
					true when is_binary(Key) ->
						Key;
					true when is_list(Key) ->
						list_to_binary(Key);
					false ->
						throw({?CB_BAD_REQUEST,<<"bad request">>})
				end,
				[{mask,Mask}]=ets:lookup(State#erlcb_client_state.bucket_table,mask),
				VBucket=vbucket_id(crc,BinKey,Mask),
				{BinKey,VBucket}
			end,
			Keys
		),
		{NewState,Result}=multi_get(Requests,State,1,length(ets:lookup_element(erlcb_pool,State#erlcb_client_state.name,#erlcb_couchbase.node))),
		{reply,Result,NewState}
	catch
		throw:Error->
			{reply,Error,State};
		_:_->
			{reply,{?CB_UNEXPECTED_ERROR,<<"unexpected error">>},State}
	end;
    
handle_call({view,{Design,View,Query}},_From,State)->
	try
		{NewState,Result}=view({Design,View,Query},State,1,length(ets:lookup_element(erlcb_pool,State#erlcb_client_state.name,#erlcb_couchbase.node))),
		{reply,erlcb_view:parse_response(Result),NewState}
	catch
		_:_->
			{reply,{?CB_UNEXPECTED_ERROR,<<"unexpected error">>},State}
	end;

handle_call({stat,_},_From,State)->
	try
		{NewState,Result}=stat(State,1,length(ets:lookup_element(erlcb_pool,State#erlcb_client_state.name,#erlcb_couchbase.node))),
		{reply,Result,NewState}
	catch
		_:_->
			{reply,{?CB_UNEXPECTED_ERROR,<<"unexpected error">>},State}
	end;
	
handle_call({Op,Request},_From,State)->
	try
		Key=element(1,Request),
		{VBucket,VBucketMap}=get_vbucket(State#erlcb_client_state.bucket_table,Key),
%		io:format("VBucket : ~p, Map :~p~n",[VBucket,VBucketMap]),
		Result=op(Op,Request,{VBucket,VBucketMap},State),
		{reply,Result,State}
	catch
		_:_->
			{reply,{?CB_UNEXPECTED_ERROR,<<"unexpected error">>},State}
	end;
	
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Op Function Definitions
%% ------------------------------------------------------------------

view({Design,View,Query},State,Index,Mask) ->
	CouchIndex=(State#erlcb_client_state.round_robin + Index) rem Mask,
	CouchApiServer=list_to_atom(State#erlcb_client_state.couchapi ++ integer_to_list(CouchIndex)),
	case gen_server:call(CouchApiServer,{fetch,{Design,View,Query}}) of
		{HttpStatus,Result} 
				when (HttpStatus =:= ?OK) 
				or (HttpStatus =:= ?NOT_FOUND)
				or (HttpStatus =:= ?UNAUTHORIZED) 
				or (HttpStatus =:= ?BAD_REQUEST) 
				or (HttpStatus =:= ?INTERNAL_SERVER_ERROR) ->
				
			{State#erlcb_client_state{round_robin=CouchIndex},{HttpStatus,Result}};
		_->
			Next=Index+1,
			case Next > Mask of
				false ->
					view({Design,View,Query},State,Next,Mask);
				true ->
					{State,{?CB_UNEXPECTED_ERROR,<<"no active server">>}}
			end
	end.
	
multi_get(Requests,State,Index,Mask)->
	ProxyIndex=(State#erlcb_client_state.round_robin + Index) rem Mask,
	ProxyServer=list_to_atom(State#erlcb_client_state.mc_proxy ++ integer_to_list(ProxyIndex)),
	{Count,Result}=gen_server:call(ProxyServer,{multi_get,{Requests}}),
	case Count < 0 of
		false ->
			{State#erlcb_client_state{round_robin=ProxyIndex},{Count,Result}};
		true ->
			Next=Index+1,
			case Next > Mask of
				false ->
					multi_get(Requests,State,Next,Mask);
				true ->
					{State,{?CB_UNEXPECTED_ERROR,<<"no active server">>}}
			end
	end.

stat(State,Index,Mask) ->
	ServerIndex=(State#erlcb_client_state.round_robin + Index) rem Mask,
	StatServer=list_to_atom(State#erlcb_client_state.mc_direct++integer_to_list(ServerIndex)),
	{Status,Result}=gen_server:call(StatServer,{stat,{}}),

	case Status < 0 of
		true ->
			Next=Index+1,
			case Next > Mask of
				false ->
					stat(State,Index+1,Mask);
				true ->
					{State#erlcb_client_state{round_robin=ServerIndex},{?CB_UNEXPECTED_ERROR,<<"no active server">>}}
			end;
		false ->
			{State#erlcb_client_state{round_robin=ServerIndex},{Status,Result}}
	end.
	

op(Op,Request,{VBucket,VBucketMap},State)->
	case VBucketMap of
		[] ->
			{?CB_UNEXPECTED_VBUCKET_ERROR,<<"Unexpected vbucket error">>};
		[Map|Next] ->
			case Map >= 0 of
				false ->
					op(Op,Request,{VBucket,Next},State);
				true ->
					DirectServer=list_to_atom(State#erlcb_client_state.mc_direct++integer_to_list(Map)),
					{Status,Result}=gen_server:call(DirectServer,{Op,{Request,VBucket}}),
					case Status < 0 of
						true ->
							op(Op,Request,{VBucket,Next},State);
						false ->
							case Status of
								?NOT_MY_VBUCKET ->
									op(Op,Request,{VBucket,Next},State);
								_->
									{Status,Result}
							end
					end
			end
	end.	
	
%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
get_vbucket(BucketTable,Key)->
	[{mask,Mask}]=ets:lookup(BucketTable,mask),
	VBucket=vbucket_id(crc,Key,Mask),
	[{_,VBucketMap}]=ets:lookup(BucketTable,VBucket),
	{VBucket,VBucketMap}.
		
vbucket_id(crc,Key,Mask)->
	%(((erlang:crc32(Key) bsr 16) band 16#7fff ) band 16#ffffffff ) band Mask.
	((erlang:crc32(Key) bsr 16) band 16#7fff ) rem Mask.
	

