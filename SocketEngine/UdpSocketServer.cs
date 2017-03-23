using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SuperSocket.Common;
using SuperSocket.SocketBase;
using SuperSocket.SocketBase.Command;
using SuperSocket.SocketBase.Protocol;
using SuperSocket.SocketEngine.AsyncSocket;
using LiteNetLib;
using LiteNetLib.Utils;
using SuperSocket.SocketBase.Logging;
using SuperSocket.SocketBase.Config;

namespace SuperSocket.SocketEngine
{
    class UdpSocketServer<TRequestInfo> : SocketServerBase, IActiveConnector
        where TRequestInfo : IRequestInfo
    {
        private IPEndPoint m_EndPointIPv4;

        private IPEndPoint m_EndPointIPv6;

        private bool m_IsUdpRequestInfo = false;

        private IReceiveFilter<TRequestInfo> m_UdpRequestFilter;

        private int m_ConnectionCount = 0;

        private IRequestHandler<TRequestInfo> m_RequestHandler;

        private INetEventListener liteServerListener;

        private NetManager server;

        /// <summary>
        /// Initializes a new instance of the <see cref="UdpSocketServer&lt;TRequestInfo&gt;"/> class.
        /// </summary>
        /// <param name="appServer">The app server.</param>
        /// <param name="listeners">The listeners.</param>
        public UdpSocketServer(IAppServer appServer, ListenerInfo[] listeners)
            : base(appServer, listeners)
        {
            m_RequestHandler = appServer as IRequestHandler<TRequestInfo>;

            m_EndPointIPv4 = new IPEndPoint(IPAddress.Any, 0);
            m_EndPointIPv6 = new IPEndPoint(IPAddress.IPv6Any, 0);

            m_IsUdpRequestInfo = typeof(TRequestInfo).IsSubclassOf(typeof(UdpRequestInfo));

            m_UdpRequestFilter = ((IReceiveFilterFactory<TRequestInfo>)appServer.ReceiveFilterFactory).CreateFilter(appServer, null, null);

            liteServerListener = new ServerListener<TRequestInfo>(appServer, m_RequestHandler, m_UdpRequestFilter);
            server = new NetManager(liteServerListener, appServer.Config.MaxConnectionNumber /* maximum clients */, "SomeConnectionKey");
            server.Start(appServer.Config.Port);
        }


        public override bool Start()
        {
            IsStopped = false;
            ILog log = AppServer.Logger;

            var config = AppServer.Config;

            server.Start(config.Port);

            IsRunning = true;
            return true;
        }

        /// <summary>
        /// Called when [new client accepted].
        /// </summary>
        /// <param name="listener">The listener.</param>
        /// <param name="client">The client.</param>
        /// <param name="state">The state.</param>
        protected override void OnNewClientAccepted(ISocketListener listener, Socket client, object state)
        {
            var paramArray = state as object[];

            var receivedData = paramArray[0] as byte[];
            var socketAddress = paramArray[1] as SocketAddress;
            var remoteEndPoint = (socketAddress.Family == AddressFamily.InterNetworkV6 ? m_EndPointIPv6.Create(socketAddress) : m_EndPointIPv4.Create(socketAddress)) as IPEndPoint;

            try
            {
                if (m_IsUdpRequestInfo)
                {
                    ProcessPackageWithSessionID(client, remoteEndPoint, receivedData);
                }
                else
                {
                    ProcessPackageWithoutSessionID(client, remoteEndPoint, receivedData);
                }
            }
            catch (Exception e)
            {
                if (AppServer.Logger.IsErrorEnabled)
                    AppServer.Logger.Error("Process UDP package error!", e);
            }
        }

        IAppSession CreateNewSession(Socket listenSocket, IPEndPoint remoteEndPoint, string sessionID)
        {
            if (!DetectConnectionNumber(remoteEndPoint))
                return null;

            var socketSession = new UdpSocketSession(listenSocket, remoteEndPoint, sessionID);
            var appSession = AppServer.CreateAppSession(socketSession);

            if (appSession == null)
                return null;

            if (!DetectConnectionNumber(remoteEndPoint))
                return null;

            if (!AppServer.RegisterSession(appSession))
                return null;

            Interlocked.Increment(ref m_ConnectionCount);

            socketSession.Closed += OnSocketSessionClosed;
            socketSession.Start();

            return appSession;
        }


        void ProcessPackageWithSessionID(Socket listenSocket, IPEndPoint remoteEndPoint, byte[] receivedData)
        {
            TRequestInfo requestInfo;
            
            string sessionID;

            int rest;

            try
            {
                requestInfo = this.m_UdpRequestFilter.Filter(receivedData, 0, receivedData.Length, false, out rest);
            }
            catch (Exception exc)
            {
                if(AppServer.Logger.IsErrorEnabled)
                    AppServer.Logger.Error("Failed to parse UDP package!", exc);
                return;
            }

            var udpRequestInfo = requestInfo as UdpRequestInfo;

            if (rest > 0)
            {
                if (AppServer.Logger.IsErrorEnabled)
                    AppServer.Logger.Error("The output parameter rest must be zero in this case!");
                return;
            }

            if (udpRequestInfo == null)
            {
                if (AppServer.Logger.IsErrorEnabled)
                    AppServer.Logger.Error("Invalid UDP package format!");
                return;
            }

            if (string.IsNullOrEmpty(udpRequestInfo.SessionID))
            {
                if (AppServer.Logger.IsErrorEnabled)
                    AppServer.Logger.Error("Failed to get session key from UDP package!");
                return;
            }

            sessionID = udpRequestInfo.SessionID;

            var appSession = AppServer.GetSessionByID(sessionID);

            if (appSession == null)
            {
                appSession = CreateNewSession(listenSocket, remoteEndPoint, sessionID);

                //Failed to create a new session
                if (appSession == null)
                    return;
            }
            else
            {
                var socketSession = appSession.SocketSession as UdpSocketSession;
                //Client remote endpoint may change, so update session to ensure the server can find client correctly
                socketSession.UpdateRemoteEndPoint(remoteEndPoint);
            }

            m_RequestHandler.ExecuteCommand(appSession, requestInfo);
        }

        void ProcessPackageWithoutSessionID(Socket listenSocket, IPEndPoint remoteEndPoint, byte[] receivedData)
        {
            var sessionID = remoteEndPoint.ToString();
            var appSession = AppServer.GetSessionByID(sessionID);

            if (appSession == null) //New session
            {
                appSession = CreateNewSession(listenSocket, remoteEndPoint, sessionID);

                //Failed to create a new session
                if (appSession == null)
                    return;

                appSession.ProcessRequest(receivedData, 0, receivedData.Length, false);
            }
            else //Existing session
            {
                appSession.ProcessRequest(receivedData, 0, receivedData.Length, false);
            }
        }

        void OnSocketSessionClosed(ISocketSession socketSession, CloseReason closeReason)
        {
            Interlocked.Decrement(ref m_ConnectionCount);
        }

        bool DetectConnectionNumber(EndPoint remoteEndPoint)
        {
            if (m_ConnectionCount >= AppServer.Config.MaxConnectionNumber)
            {
                if (AppServer.Logger.IsErrorEnabled)
                    AppServer.Logger.ErrorFormat("Cannot accept a new UDP connection from {0}, the max connection number {1} has been exceed!",
                        remoteEndPoint.ToString(), AppServer.Config.MaxConnectionNumber);

                return false;
            }

            return true;
        }

        protected override ISocketListener CreateListener(ListenerInfo listenerInfo)
        {
            return new UdpSocketListener(listenerInfo);
        }

        public override void ResetSessionSecurity(IAppSession session, System.Security.Authentication.SslProtocols security)
        {
            throw new NotSupportedException();
        }

        Task<ActiveConnectResult> IActiveConnector.ActiveConnect(EndPoint targetEndPoint)
        {
            return ((IActiveConnector)this).ActiveConnect(targetEndPoint, null);
        }

        Task<ActiveConnectResult> IActiveConnector.ActiveConnect(EndPoint targetEndPoint, EndPoint localEndPoint)
        {
            var taskSource = new TaskCompletionSource<ActiveConnectResult>();
            var socket = new Socket(targetEndPoint.AddressFamily, SocketType.Dgram, ProtocolType.Udp);

            if (localEndPoint != null)
            {
                socket.ExclusiveAddressUse = false;
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                socket.Bind(localEndPoint);
            }

            var session = CreateNewSession(socket, (IPEndPoint)targetEndPoint, targetEndPoint.ToString());

            if (session == null)
                taskSource.SetException(new Exception("Failed to create session for this socket."));
            else
                taskSource.SetResult(new ActiveConnectResult { Result = true, Session = session });

            return taskSource.Task;
        }
    }

    class ServerListener<TRequestInfo> : INetEventListener where TRequestInfo : IRequestInfo
    {
        private IAppServer appServer;
        private IRequestHandler<TRequestInfo> m_RequestHandler;
        private IReceiveFilter<TRequestInfo> m_UdpRequestFilter;
        private Dictionary<NetPeer,IAppSession> sessions = new Dictionary<NetPeer, IAppSession>();

        public ServerListener(IAppServer appServer, IRequestHandler<TRequestInfo> m_RequestHandler, IReceiveFilter<TRequestInfo> m_UdpRequestFilter)
        {
            this.appServer = appServer;
            this.m_RequestHandler = m_RequestHandler;
            this.m_UdpRequestFilter = m_UdpRequestFilter;
        }
        public void OnPeerConnected(NetPeer peer)
        {
            Console.WriteLine("[Server] Peer connected: " + peer.EndPoint);
            IPEndPoint endpoint = new IPEndPoint(IPAddress.Parse(peer.EndPoint.Host), peer.EndPoint.Port);
            IAppSession appSession = appServer.CreateAppSessionFromEndpoint(endpoint);
            sessions.Add(peer, appSession);
            //var peers = Server.GetPeers();
            //foreach (var netPeer in peers)
            //{
            //    Console.WriteLine("ConnectedPeersList: id={0}, ep={1}", netPeer.ConnectId, netPeer.EndPoint);
            //}
        }

        public void OnPeerDisconnected(NetPeer peer, DisconnectInfo disconnectInfo)
        {
            Console.WriteLine("[Server] Peer disconnected: " + peer.EndPoint + ", reason: " + disconnectInfo.Reason);
            sessions.Remove(peer);
        }

        public void OnNetworkError(NetEndPoint endPoint, int socketErrorCode)
        {
            Console.WriteLine("[Server] error: " + socketErrorCode);
        }

        public void OnNetworkReceive(NetPeer peer, NetDataReader reader)
        {
            TRequestInfo requestInfo;

            int rest;
            try
            {
                requestInfo = this.m_UdpRequestFilter.Filter(reader.Data, 0, reader.Data.Length, false, out rest);
            }
            catch (Exception exc)
            {
                //if(AppServer.Logger.IsErrorEnabled)
                    Console.WriteLine("Failed to parse UDP package!", exc);
                return;
            }

            var udpRequestInfo = requestInfo as UdpRequestInfo;

            if (rest > 0)
            {
                //if (AppServer.Logger.IsErrorEnabled)
                    Console.WriteLine("The output parameter rest must be zero in this case!");
                return;
            }

            if (udpRequestInfo == null)
            {
                //if (AppServer.Logger.IsErrorEnabled)
                    Console.WriteLine("Invalid UDP package format!");
                return;
            }
            IAppSession appSession = sessions[peer];
            m_RequestHandler.ExecuteCommand(appSession, requestInfo);
            //echo
            peer.Send(reader.Data, SendOptions.ReliableUnordered);

            //fragment log
            if (reader.AvailableBytes == 13218)
            {
                Console.WriteLine("[Server] TestFrag: {0}, {1}", reader.Data[0], reader.Data[13217]);
            }
        }

        public void OnNetworkReceiveUnconnected(NetEndPoint remoteEndPoint, NetDataReader reader, UnconnectedMessageType messageType)
        {
            Console.WriteLine("[Server] ReceiveUnconnected: {0}", reader.GetString(100));
        }

        public void OnNetworkLatencyUpdate(NetPeer peer, int latency)
        {

        }
    }
}
