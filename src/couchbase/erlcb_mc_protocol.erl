%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc memcache binary protocol
%%
-module(erlcb_mc_protocol).
-author('Chae,H B<chaehb@gmail.com>').

-export([connect/1, op/3]).

-export([encode_packet/2]).

-include("mc_constants.hrl").
-include("erlcb_memcache.hrl").

%% ------------------------------------------------------------------
%% Connect Socket
%% ------------------------------------------------------------------

connect([Host,Port,BucketName,BucketPassword])->
	case gen_tcp:connect(Host,Port,[binary,{packet,0},{active,false}],1000) of
		{ok,Socket}->
			io:format("Connect to ~s:~w~n",[Host,Port]),
			%% if BucketPassword is not set or "",pass authentication 
			case BucketPassword of
				undefined ->
					#erlcb_memcache{host=Host,port=Port,bucket=BucketName,password=[],connected=true,socket=Socket};
				[] ->
					#erlcb_memcache{host=Host,port=Port,bucket=BucketName,password=[],connected=true,socket=Socket};
				Password ->
					InitState=#erlcb_memcache{host=Host,port=Port,bucket=BucketName,password=Password,connected=true,socket=Socket},
					%% todo : process auth error
					case authenticate(InitState) of
						{ok,auth} ->
							InitState;
						{error,_} ->
							InitState#erlcb_memcache{connected=false}
					end
			end;
		{error,_Reason} ->
			#erlcb_memcache{host=Host,port=Port,bucket=BucketName,password=BucketPassword,connected=false}
	end.

%% ------------------------------------------------------------------
%% Operations
%% ------------------------------------------------------------------	
op(stat,{},State)->
	RequestBody=#mc_body{
	},
	RequestHeader=#mc_header{
		magic=?REQ_MAGIC,
		opcode=?STAT
	},
	send_recv(State,encode_packet(RequestHeader,RequestBody));
	
op(get,{Request,VBucket},State)->
	Key=element(1,Request),
	RequestBody=#mc_body{
		key=Key
	},
	RequestHeader=#mc_header{
		magic=?REQ_MAGIC,
		opcode=?GET,
		keylen=size(Key),
		reserve=VBucket,
		bodylen=size(Key)
	},
	send_recv(State,encode_packet(RequestHeader,RequestBody));
		
op(add,{Request,VBucket},State)->
	{Key,Expiry,Value}=Request,

	Extras= <<16#deadbeef:32, Expiry:32>>,
	RequestBody=#mc_body{
		extras=Extras,
		key=Key,
		data=Value
	},
	RequestHeader=#mc_header{
		magic=?REQ_MAGIC,
		opcode=?ADD,
		keylen=size(Key),
		extraslen=size(Extras),
		reserve=VBucket
	},
	send_recv(State,encode_packet(RequestHeader,RequestBody));
		
op(set,{Request,VBucket},State)->
	{Key,Expiry,Value}=Request,

	Extras= <<16#deadbeef:32, Expiry:32>>,
	RequestBody=#mc_body{
		extras=Extras,
		key=Key,
		data=Value
	},
	RequestHeader=#mc_header{
		magic=?REQ_MAGIC,
		opcode=?SET,
		keylen=size(Key),
		extraslen=size(Extras),
		reserve=VBucket
	},
	send_recv(State,encode_packet(RequestHeader,RequestBody));
		
op(replace,{Request,VBucket},State)->
	{Key,Expiry,Value}=Request,

	Extras= <<16#deadbeef:32, Expiry:32>>,
	RequestBody=#mc_body{
		extras=Extras,
		key=Key,
		data=Value
	},
	RequestHeader=#mc_header{
		magic=?REQ_MAGIC,
		opcode=?REPLACE,
		keylen=size(Key),
		extraslen=size(Extras),
		reserve=VBucket
	},
	send_recv(State,encode_packet(RequestHeader,RequestBody));
	
op(delete,{Request,VBucket},State)->
	Key=element(1,Request),
	RequestBody=#mc_body{
		key=Key
	},
	RequestHeader=#mc_header{
		magic=?REQ_MAGIC,
		opcode=?DELETE,
		keylen=size(Key),
		reserve=VBucket,
		bodylen=size(Key)
	},
	send_recv(State,encode_packet(RequestHeader,RequestBody));

op(touch,{Request,VBucket},State)->
	{Key,Expiry}=Request,
	
	Extras= <<Expiry:32>>,
	RequestBody=#mc_body{
		extras=Extras,
		key=Key
	},
	RequestHeader=#mc_header{
		magic=?REQ_MAGIC,
		opcode=?TOUCH,
		keylen=size(Key),
		extraslen=size(Extras),
		reserve=VBucket
	},
	send_recv(State,encode_packet(RequestHeader,RequestBody));

op(get_and_touch,{Request,VBucket},State)->
	{Key,Expiry}=Request,
	
	Extras= <<Expiry:32>>,
	RequestBody=#mc_body{
		extras=Extras,
		key=Key
	},
	RequestHeader=#mc_header{
		magic=?REQ_MAGIC,
		opcode=?GET_AND_TOUCH,
		keylen=size(Key),
		extraslen=size(Extras),
		reserve=VBucket
	},
	send_recv(State,encode_packet(RequestHeader,RequestBody));

op(_,_,State)->
	{reply,{?CB_UNKNOWN_COMMAND,<<"unknown command">>},State}.
		
		
%% ------------------------------------------------------------------
%% Socket Authenticate
%% ------------------------------------------------------------------

authenticate(State) ->
	Body=#mc_body{},
	Header=#mc_header{
		magic=?REQ_MAGIC,
		opcode=?CMD_SASL_LIST_MECHS
	},
	NewState=send(State,encode_packet(Header,Body)),
	
	{ok,{_ResHeader,ResBody}}=recv(NewState),
	case ResBody#mc_body.data of
		<<"PLAIN">> ->
			request_auth(State);
		_->
			{error,unsupported_method}
	end.

request_auth(State)->
	BucketName=list_to_binary(State#erlcb_memcache.bucket),
	BucketPassword=list_to_binary(State#erlcb_memcache.password),
	Body=#mc_body{key= <<"PLAIN">>,data= <<BucketName/binary,16#00:8,BucketName/binary,16#00:8,BucketPassword/binary>>},
	Header=#mc_header{
		magic=?REQ_MAGIC,
		opcode=?CMD_SASL_AUTH,
		keylen=size(Body#mc_body.key)
	},
	NewState=send(State,encode_packet(Header,Body)),
	{ok,{_ResHeader,ResBody}}=recv(NewState),
	case ResBody#mc_body.data of
		<<"Authenticated">> ->
			{ok,auth};
		_ ->
			{error,auth_failed}
	end.

%% ------------------------------------------------------------------
%% Packet Encoding
%% ------------------------------------------------------------------

encode_packet(Header,Body)->
	BodyPacket=encode_body(Body),
	BodyLen=size(BodyPacket),
	<<(Header#mc_header.magic):8,
	(Header#mc_header.opcode):8,
	(Header#mc_header.keylen):16,
	(Header#mc_header.extraslen):8,
	(Header#mc_header.datatype):8,
	(Header#mc_header.reserve):16,
	BodyLen:32,
	(Header#mc_header.opaque):32,
	(Header#mc_header.cas):64,
	BodyPacket:BodyLen/binary>>.

encode_body(Body)->
	ExtrasSize=size(Body#mc_body.extras),
	<<(Body#mc_body.extras):ExtrasSize/binary,(Body#mc_body.key)/binary,(Body#mc_body.data)/binary>>.
	
%% ------------------------------------------------------------------
%% Socket Communications
%% ------------------------------------------------------------------
send_recv(State,Request)->
	NewState=send(State,Request),
	case NewState#erlcb_memcache.connected of
		true ->
			{ResultState,Result}=recv(NewState),
			case ResultState of
				ok ->
					{ResHeader,ResBody}=Result,
					try
						%{reply,{ResHeader#mc_header.reserve,jiffy:decode(ResBody#mc_body.data)},NewState}
						{reply,{ResHeader#mc_header.reserve,ResBody#mc_body.data},NewState}
					catch
						_:_->
						{reply,{ResHeader#mc_header.reserve,ResBody#mc_body.data},NewState}
					end;					
				error ->
					case Result of
						closed ->
							RenewState=send(State,Request),
							case RenewState#erlcb_memcache.connected of
								true ->
									{NewResultState,NewResult}=recv(RenewState),
									case NewResultState of
										ok ->
											{NewResHeader,NewResBody}=NewResult,
											try
												%{reply,{NewResHeader#mc_header.reserve,jiffy:decode(NewResBody#mc_body.data)},RenewState}
												{reply,{NewResHeader#mc_header.reserve, NewResBody#mc_body.data},RenewState}
											catch
												_:_->
												{reply,{NewResHeader#mc_header.reserve,NewResBody#mc_body.data},RenewState}
											end;
										error ->
											{reply,{?CB_RECV_ERROR,NewResult},RenewState}
									end;
								false ->
									{reply,{?CB_NOT_CONNECTED,<<>>},RenewState}
							end;
						_->
							{reply,{?CB_RECV_ERROR,Result},NewState}
					end
			end;
		false ->
			{reply,{?CB_NOT_CONNECTED,<<>>},NewState}
	end.

send(State,Request)->
	try gen_tcp:send(State#erlcb_memcache.socket,Request) of
		ok ->
			State;
		{error,_Reason} ->
			NewState=connect([State#erlcb_memcache.host,State#erlcb_memcache.port,State#erlcb_memcache.bucket,State#erlcb_memcache.password]),
			case NewState#erlcb_memcache.connected of
				true ->
					gen_tcp:send(NewState#erlcb_memcache.socket,Request);
				false->
					ok
			end,
			NewState
		catch
			_:_->
				NewState=connect([State#erlcb_memcache.host,State#erlcb_memcache.port,State#erlcb_memcache.bucket,State#erlcb_memcache.password]),
				case NewState#erlcb_memcache.connected of
					true ->
						gen_tcp:send(NewState#erlcb_memcache.socket,Request);
					false->
						ok
				end,
				NewState
	end.

recv(State)->
	case recv_header(State#erlcb_memcache.socket) of
		{error,Error} ->
			{error,Error};
		Header ->
			Body=recv_body(State#erlcb_memcache.socket,Header),
			{ok,{Header,Body}}
	end.

recv_header(Socket) ->
	case recv_bytes(Socket,?HEADER_LEN) of
		{error,Error} ->
			{error,Error};
		<<?RES_MAGIC:8, Opcode:8, KeyLen:16, ExtrasLen:8, DataType:8, Status:16, BodyLen:32, Opaque:32, CAS:64>> ->
			#mc_header{
				magic=?RES_MAGIC,
				opcode=Opcode,
				keylen=KeyLen,
				extraslen=ExtrasLen,
				datatype=DataType,
				reserve=Status,
				bodylen=BodyLen,
				opaque=Opaque,
				cas=CAS
			}
	end.

recv_body(Socket,Header)->
	BodyLen=Header#mc_header.bodylen,
	case recv_bytes(Socket,BodyLen) of
		{error,Error}->
			{error,Error};
		Bin ->
			ExtrasSize=Header#mc_header.extraslen,
			KeySize=Header#mc_header.keylen,
			<<Extras:ExtrasSize/binary,Key:KeySize/binary,Value/binary>> = Bin,
			#mc_body{extras=Extras,key=Key,data=Value}
	end.

recv_bytes(_,0) -> 
	<<>>;
recv_bytes(Socket,NumBytes)->
	case gen_tcp:recv(Socket, NumBytes) of
		{ok,Bin} -> 
			Bin;
		Error ->
			Error
	end.
