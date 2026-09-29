// 原生 TLS 测试:节点 DOCSQL_TLS_CERT/KEY 监听 + 连接串 tls=true。
// 覆盖:TLS 全功能 SQL 往返(含池化重开)、明文客户端被响亮拒绝、
// tls_ca 信任锚验证(同证书自验通过)、错误 CA 拒绝握手。

using Docsql.Client;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Xunit;

public sealed class NativeTlsServer : IDisposable
{
    public readonly Process Proc;
    private readonly string _db;
    private readonly string _cert;
    private readonly string _key;
    public int Port { get; }
    public string Cs => $"host=127.0.0.1;port={Port};tls=true";
    public string CertPath => _cert;

    private NativeTlsServer(Process proc, string db, string cert, string key, int port) =>
        (Proc, _db, _cert, _key, Port) = (proc, db, cert, key, port);

    /// <summary>生成带 DNS:localhost + IP:127.0.0.1 SAN 的自签证书(tls_ca
    /// 验证用例需要按 IP 拨号时名称匹配成立)。</summary>
    public static (string CertPem, string KeyPem) WriteSelfSigned(string certPath, string keyPath)
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(
            "CN=docsql-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        req.CertificateExtensions.Add(san.Build());
        using var cert = req.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(5));
        var certPem = cert.ExportCertificatePem();
        var keyPem = rsa.ExportPkcs8PrivateKeyPem();
        File.WriteAllText(certPath, certPem);
        File.WriteAllText(keyPath, keyPem);
        return (certPem, keyPem);
    }

    public static NativeTlsServer Start()
    {
        using var l = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        if (!File.Exists(exe))
            throw new FileNotFoundException("docsql-server 不存在:" + exe);
        var dir = Path.Combine(Path.GetTempPath(), $"docsql-tls-{port}");
        Directory.CreateDirectory(dir);
        var cert = Path.Combine(dir, "cert.pem");
        var key = Path.Combine(dir, "key.pem");
        WriteSelfSigned(cert, key);
        var db = Path.Combine(dir, "node.db");
        var psi = new ProcessStartInfo
        {
            FileName = exe, ArgumentList = { db, $"127.0.0.1:{port}" },
            CreateNoWindow = true, RedirectStandardError = false,
        };
        psi.Environment["DOCSQL_TLS_CERT"] = cert;
        psi.Environment["DOCSQL_TLS_KEY"] = key;
        var proc = Process.Start(psi) ?? throw new InvalidOperationException("启动失败");
        for (var i = 0; i < 100; i++)
        {
            try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", port); return new(proc, db, cert, key, port); }
            catch { Thread.Sleep(50); }
        }
        throw new InvalidOperationException("启动超时");
    }

    public void Dispose()
    {
        try { if (!Proc.HasExited) Proc.Kill(); } catch { }
        Proc.Dispose();
        try { Directory.Delete(Path.GetDirectoryName(_db)!, true); } catch { }
    }
}

public sealed class TlsTests
{
    [Fact]
    public void Tls_roundtrip_sql_and_pool_reopen()
    {
        using var server = NativeTlsServer.Start();
        using var conn = new DocsqlConnection(server.Cs);
        conn.Open();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE tls_t (id INT PRIMARY KEY, v TEXT)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO tls_t VALUES (1, '加密行')";
            cmd.ExecuteNonQuery();
        }
        // Close→Open 走池化复用同一条 TLS 物理连接(握手不能被重复要求)。
        conn.Close();
        conn.Open();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM tls_t";
            Assert.Equal(1L, Convert.ToInt64(cmd.ExecuteScalar()));
        }
    }

    [Fact]
    public void Plaintext_client_is_refused_by_tls_listener()
    {
        using var server = NativeTlsServer.Start();
        // 不带 tls=true 的连接串 = 明文拨号。TCP 三次握手本身会成功,
        // 匿名 Open 也不发帧 —— 拒绝发生在服务端 TLS 握手失败弃连之后:
        // 首次命令读必须响亮抛错(EOF),而不是把 TLS 告警字节当协议帧。
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={server.Port}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        Assert.ThrowsAny<Exception>(() => cmd.ExecuteScalar());
    }

    [Fact]
    public void Tls_ca_trust_anchor_validates_the_same_certificate()
    {
        using var server = NativeTlsServer.Start();
        var cs = $"host=127.0.0.1;port={server.Port};tls=true;tls_ca={server.CertPath}";
        using var conn = new DocsqlConnection(cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 + 1";
        Assert.Equal(2L, Convert.ToInt64(cmd.ExecuteScalar()));
    }

    [Fact]
    public void Tls_wrong_ca_rejects_the_handshake()
    {
        using var server = NativeTlsServer.Start();
        // 另一张无关自签证书作信任锚:链验证必须失败(握手即拒,不开连接)。
        var other = Path.Combine(Path.GetTempPath(), $"docsql-other-ca-{System.Guid.NewGuid():N}.pem");
        var otherKey = Path.Combine(Path.GetTempPath(), $"docsql-other-ca-{System.Guid.NewGuid():N}.key");
        try
        {
            NativeTlsServer.WriteSelfSigned(other, otherKey);
            var cs = $"host=127.0.0.1;port={server.Port};tls=true;tls_ca={other}";
            using var conn = new DocsqlConnection(cs);
            Assert.ThrowsAny<Exception>(() => conn.Open());
        }
        finally
        {
            try { File.Delete(other); } catch { }
            try { File.Delete(otherKey); } catch { }
        }
    }
}
