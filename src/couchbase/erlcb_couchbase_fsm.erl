%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc gen_fsm for check connection state and update notification
%%
-module(erlcb_couchbase_fsm).

-author('Chae,H B<chaehb@gmail.com>').

-behaviour(gen_fsm).

-include("erlcb.hrl").

-define(STREAM_CHUNK_END, <<"\n\n\n\n">>).

-record(erlcb_couchbase_fsm,{
	name,
	node,
	pid
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

%% ------------------------------------------------------------------
%% gen_fsm Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3,
         code_change/4]).

-export([idle/2]).

-export([lookupStream/1,lookupStream/2]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(CouchbaseName) ->
	ModuleName=erlcb_util:append_to_atom(CouchbaseName,'_fsm'),
    gen_fsm:start_link({local, ModuleName}, ?MODULE, [CouchbaseName], []).

%% ------------------------------------------------------------------
%% gen_fsm Function Definitions
%% ------------------------------------------------------------------

init([CouchbaseName]) ->
	process_flag(trap_exit, true),
	
	Pid=spawn(?MODULE,lookupStream,[CouchbaseName]),

    {ok, idle, #erlcb_couchbase_fsm{name=CouchbaseName,node=1,pid=Pid}}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, State) ->
	clean_couchbase(State#erlcb_couchbase_fsm.name),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ------------------------------------------------------------------
%% States Definitions
%% ------------------------------------------------------------------
	
idle(restart,State) ->
	io:format("Restart streaming : ~p~n",[State#erlcb_couchbase_fsm.name]),
	
	[Couchbase]=ets:lookup(erlcb_pool,State#erlcb_couchbase_fsm.name),
	case length(Couchbase#erlcb_couchbase.node) of
		0 ->
			{next_state, restart, State};% retry until connected
		NodeLen ->
			case State#erlcb_couchbase_fsm.pid of
				undefined ->
					ok;
				OldPid when is_pid(OldPid) ->
					exit(OldPid,kill);
				_->
					ok
			end,
			
			NodeIndex=State#erlcb_couchbase_fsm.node,
			Pid=spawn(?MODULE,lookupStream,[Couchbase,NodeIndex]),
			
			NextNodeIndex=((NodeIndex+1) rem NodeLen)+1,
		
			{next_state, idle,State#erlcb_couchbase_fsm{pid=Pid,node=NextNodeIndex}}
	end;

%idle(restart,State) when State#erlcb_couchbase_fsm.streaming_running == true ->
%	io:format("already streaming ...~n",[]),
%	{next_state,idle,State};

idle(idle,State)->
	io:format("wait...~n",[]),
	{next_state,idle,State}.
	
	
%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% Receive Streaming
%% ------------------------------------------------------------------
lookupStream(CouchbaseName) ->
	io:format("Start streaming : ~p~n",[CouchbaseName]),
	[Config]=ets:lookup(erlcb_pool,CouchbaseName),

	[BucketName,BucketPassword]=Config#erlcb_couchbase.bucket,

	Url=atom_to_list(Config#erlcb_couchbase.protocol) ++"://"++ Config#erlcb_couchbase.host++":"++ integer_to_list(Config#erlcb_couchbase.port)++ Config#erlcb_couchbase.streamingUri,

	Password=case BucketPassword of
		undefined ->
			[];
		_->
			BucketPassword
	end,
	Headers=erlcb_http:basic_auth_header(BucketName,Password),

	SSLOptions = case string:str(Url,"https://") of
		1 ->
			erlcb_http:https_option();
		_->
			[]
	end,
		
	try httpc:request(get,{Url,Headers},SSLOptions,[{sync,false},{stream,self}]) of
		{ok,RequestId} ->
			receive_data(CouchbaseName,RequestId);
		_->
			io:format("streaming connection closed.~n Now Restart...~n",[]),
			gen_fsm:send_event(erlcb_util:append_to_atom(CouchbaseName,'_fsm'),restart)
	catch
		_:Reason ->
			io:format("streaming error : ~p~n",[Reason]),
			gen_fsm:send_event(erlcb_util:append_to_atom(CouchbaseName,'_fsm'),restart)
	end.
	

lookupStream(Couchbase,NodeIndex) ->
	CouchbaseName = Couchbase#erlcb_couchbase.name,
	io:format("Restart streaming : ~p from ~p ~n",[CouchbaseName,NodeIndex]),
	
	Node=lists:nth(NodeIndex,Couchbase#erlcb_couchbase.node),

	[BucketName,BucketPassword]=Couchbase#erlcb_couchbase.bucket,

	Url=atom_to_list(Node#erlcb_node.protocol) ++"://"++ Node#erlcb_node.host++":"++ integer_to_list(Node#erlcb_node.port)++ Couchbase#erlcb_couchbase.streamingUri,

	Password=case BucketPassword of
		undefined ->
			[];
		_->
			BucketPassword
	end,
	Headers=erlcb_http:basic_auth_header(BucketName,Password),

	SSLOptions = case string:str(Url,"https://") of
		1 ->
			erlcb_http:https_option();
		_->
			[]
	end,
		
	try httpc:request(get,{Url,Headers},SSLOptions,[{sync,false},{stream,self}]) of
		{ok,RequestId} ->
			receive_data(CouchbaseName,RequestId);
		_->
			io:format("streaming connection closed.~n Now Restart...~n",[]),
			gen_fsm:send_event(erlcb_util:append_to_atom(CouchbaseName,'_fsm'),restart)
	catch
		_:Reason ->
			io:format("streaming error : ~p~n",[Reason]),
			gen_fsm:send_event(erlcb_util:append_to_atom(CouchbaseName,'_fsm'),restart)
	end.
	
receive_data(CouchbaseName,RequestId) ->
	case receive_chunk(CouchbaseName, RequestId) of
		{ok,_} ->
			io:format("streaming end~n",[]),
			gen_fsm:send_event(erlcb_util:append_to_atom(CouchbaseName,'_fsm'),restart);
		{error,Reason} ->
			io:format("streaming error : ~p~n",[Reason]),
			gen_fsm:send_event(erlcb_util:append_to_atom(CouchbaseName,'_fsm'),restart)
	end.

receive_chunk(CouchbaseName,RequestId)->
	receive
		{http, {RequestId, {error, Reason}}} when(Reason =:= etimedout) orelse(Reason =:= timeout) -> 
			{error,timeout};
		{http, {RequestId, {{_, 401, _} = Status, Headers, _}}} -> 
			{error, unauthorized, {Status, Headers}};
		{http, {RequestId, Result}} -> 
			{error,Result};
		{http,{RequestId, stream_start, _Headers}} ->
			receive_chunk(CouchbaseName,RequestId);
		{http,{RequestId, stream, Data}} ->
			case Data of
				?STREAM_CHUNK_END ->
					ok;
				_->
					set_vbucket(CouchbaseName,Data)
			end,
			receive_chunk(CouchbaseName,RequestId);
		{http,{RequestId, stream_end, _Headers}} ->
			{ok,stream_end}
	end.
%% ------------------------------------------------------------------
%% Setup vbucket
%% ------------------------------------------------------------------
set_vbucket(CouchbaseName,DataSource) when is_binary(DataSource) ->

	{Data}=jiffy:decode(DataSource),
	StreamingUri=binary_to_list(proplists:get_value(<<"streamingUri">>,Data)),

	{VBucketServerMap}=proplists:get_value(<<"vBucketServerMap">>,Data),
	
	VBucketServerList=jiffy:encode(proplists:get_value(<<"serverList">>,VBucketServerMap)),
	VBucketMap=jiffy:encode(proplists:get_value(<<"vBucketMap">>,VBucketServerMap)),
	
	NewChecksum=erlang:crc32(<<VBucketServerList/binary,VBucketMap/binary>>),
	
	CouchbaseData=try ets:lookup(erlcb_pool,CouchbaseName) of
			undefined ->
				#erlcb_couchbase{name=CouchbaseName};
			[] ->
				#erlcb_couchbase{name=CouchbaseName};
			[CB]->
				CB
		catch
			_:_->
				#erlcb_couchbase{name=CouchbaseName}
	end,
		
	case NewChecksum =:= CouchbaseData#erlcb_couchbase.checksum of
		true ->
			ok;
		false ->
			%% update new checksum and reset node
			ets:update_element(erlcb_pool,CouchbaseName,{#erlcb_couchbase.checksum,NewChecksum}),
			NewData=CouchbaseData#erlcb_couchbase{streamingUri=StreamingUri,checksum=NewChecksum,node=[]},
			ets:insert(erlcb_pool,NewData),
			set_vbucket(CouchbaseName,Data)
	end;
	
set_vbucket(CouchbaseName,Data ) ->
	io:format("~p : New vbucket setup...~n",[CouchbaseName]),
	
	CouchApiSup=erlcb_util:append_to_atom(CouchbaseName,'_couchapi_sup'),
	case supervisor:which_children(CouchApiSup) of
		[] ->
			ok;
		CouchApiChildren ->
			lists:foreach(
				fun({Id,_,_,_}) ->
					supervisor:terminate_child(CouchApiSup,Id),
					supervisor:delete_child(CouchApiSup,Id)
				end,
				CouchApiChildren
			)
	end,
	
	McDirectSup=erlcb_util:append_to_atom(CouchbaseName,'_mc_direct_sup'),
	case supervisor:which_children(McDirectSup) of
		[] ->
			ok;
		McDirectChildren ->
			lists:foreach(
				fun({Id,_,_,_}) ->
					supervisor:terminate_child(McDirectSup,Id),
					supervisor:delete_child(McDirectSup,Id)
				end,
				McDirectChildren
			)
	end,
	
	McProxySup=erlcb_util:append_to_atom(CouchbaseName,'_mc_proxy_sup'),
	case supervisor:which_children(McProxySup) of
		[] ->
			ok;
		McProxyChildren ->
			lists:foreach(
				fun({Id,_,_,_}) ->
					supervisor:terminate_child(McProxySup,Id),
					supervisor:delete_child(McProxySup,Id)
				end,
				McProxyChildren
			)
	end,
	
	BucketTable=erlcb_util:append_to_atom(CouchbaseName,'_vbucket'),
	case ets:info(BucketTable) of
		undefined ->
			ok;
		_->
			ets:delete(BucketTable)
	end,		
	ets:new(BucketTable,[named_table,public]),

	{VBucketServerMap}=proplists:get_value(<<"vBucketServerMap">>,Data),
	%% set vbuckets
	VBucketMap=proplists:get_value(<<"vBucketMap">>,VBucketServerMap),
	VBucketMapLength=length(VBucketMap),
	
	ets:insert(BucketTable,{mask,VBucketMapLength}),
    lists:foldl(
    	fun(MapEl,Index)->
    		ets:insert(BucketTable,{Index,MapEl}),
    		Index+1
    	end,
    	0,
    	VBucketMap
    ),
    
	%% set nodes
	[CouchServer]=ets:lookup(erlcb_pool,CouchbaseName),
	StreamingUri=binary_to_list(proplists:get_value(<<"streamingUri">>,Data)),
	
	Nodes=lists:map(
		fun({Node})->
			[Ip,_]=re:split(proplists:get_value(<<"hostname">>,Node),<<":">>),
			{Ip,Node}
		end,
		proplists:get_value(<<"nodes">>,Data)
	),
	DesignPrefix=case CouchServer#erlcb_couchbase.view of
		production ->
			"/" ;
		_->
			"/dev_"
	end,
	VBucketServerList=proplists:get_value(<<"serverList">>,VBucketServerMap),
	{NodeList,_}=lists:mapfoldl(
		fun(VBucketServer,Index)->
			[Ip,_]=re:split(VBucketServer,<<":">>),
			Node=proplists:get_value(Ip,Nodes),
			CouchApiBase=binary_to_list(proplists:get_value(<<"couchApiBase">>,Node))++"/_design"++DesignPrefix,
			Protocol = case string:str(CouchApiBase, "https") of
				1 ->
					https;
				_->
					http
			end,
			[Host,HostPort]=string:tokens(binary_to_list(proplists:get_value(<<"hostname">>,Node)),":"),
			Port=try list_to_integer(HostPort) of
					PortInt when is_integer(PortInt) ->
						PortInt;
					_->
						throw(parse_error)
				catch
					_:_->
						-1
			end,
			{MemcachePorts}=proplists:get_value(<<"ports">>,Node),
			
			DirectPort=proplists:get_value(<<"direct">>,MemcachePorts),
			ProxyPort=proplists:get_value(<<"proxy">>,MemcachePorts),
			
			ClusterMembership=binary_to_list(proplists:get_value(<<"clusterMembership">>,Node)),
			Status=binary_to_list(proplists:get_value(<<"status">>,Node)),
			
			NodeData=#erlcb_node{
				index=Index,
				protocol = Protocol,
				host=Host, 
				port=Port,
				membership= ClusterMembership,
				status= Status,
				couchApi = CouchApiBase,
				direct = DirectPort,
				proxy =ProxyPort
			},
			
			{NodeData,Index+1}
		end,
		0,
		VBucketServerList
	),
	ets:insert(erlcb_pool,CouchServer#erlcb_couchbase{streamingUri=StreamingUri,node=NodeList}),
	start_nodes(CouchbaseName, NodeList).

start_nodes(CouchbaseName,NodeList)->
	CouchApiSup=erlcb_util:append_to_atom(CouchbaseName,'_couchapi_sup'),
	McDirectSup=erlcb_util:append_to_atom(CouchbaseName,'_mc_direct_sup'),
	McProxySup=erlcb_util:append_to_atom(CouchbaseName,'_mc_proxy_sup'),

	lists:foldl(
		fun(_Node,Index) ->
			%% add memcache direct client to memcache_sup
			McDirectId=erlcb_util:append_to_atom(CouchbaseName,"_mc_direct_"++integer_to_list(Index)),
			McDirectSpec={McDirectId,{erlcb_mc_direct,start_link,[CouchbaseName,McDirectId]},permanent,5000,worker,[erlcb_mc_direct]},
			supervisor:start_child(McDirectSup,McDirectSpec),
		
			%% add memcache proxy client to memcache_sup
			McProxyId=erlcb_util:append_to_atom(CouchbaseName,"_mc_proxy_"++integer_to_list(Index)),
			McProxySpec={McProxyId,{erlcb_mc_proxy,start_link,[CouchbaseName,McProxyId]},permanent,5000,worker,[erlcb_mc_proxy]},
			supervisor:start_child(McProxySup,McProxySpec),
		
			%% add couchapi client to couchapi_sup
			CouchApiId=erlcb_util:append_to_atom(CouchbaseName,"_couchapi_"++integer_to_list(Index)),
			CouchApiSpec={CouchApiId,{erlcb_couchapi,start_link,[CouchbaseName,CouchApiId]},permanent,5000,worker,[erlcb_couchapi]},
			supervisor:start_child(CouchApiSup,CouchApiSpec),
			
			Index+1
		end,
		0,
		NodeList
	),
	ok.



clean_couchbase(CouchbaseName)->
	%% delete vbucket table
	BucketTable=erlcb_util:append_to_atom(CouchbaseName,'_vbucket'),
	case ets:info(BucketTable) of
		undefined ->
			ok;
		_->
			ets:delete(BucketTable)
	end.