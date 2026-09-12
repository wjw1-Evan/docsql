// DocSQL × Aspire 示例 AppHost:
//   - AddDocsql      单节点 DocSQL 服务器(容器,随机 token,持久化数据卷)
//   - WithWebConsole 同镜像 docsql-web 管理控制台(账号门默认开启,首次访问需设置凭据)
//   - Worker         演示消费侧:WithReference 注入连接串 + AddDocsqlConnection + 参数化写读
//
// 集群形态(对称复制,三节点互扇出,写入任一节点全网收敛):
//   var cluster = builder.AddDocsqlCluster("docsql", nodeCount: 3);
//   builder.AddProject<Projects.AspireSample_Worker>("worker")
//          .WithReference(cluster.Primary)
//          .WaitFor(cluster.Primary);

using Aspire.Hosting.Docker;
using Docsql.Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

// aspire publish 输出 docker-compose 产物(部署出口);本地 aspire start 不受影响。
builder.AddDockerComposeEnvironment("deployment");

var docsql = builder.AddDocsql("docsql")
    .WithDataVolume()
    .WithWebConsole();

builder.AddProject<Projects.AspireSample_Worker>("worker")
    .WithReference(docsql)
    .WaitFor(docsql);

builder.Build().Run();
