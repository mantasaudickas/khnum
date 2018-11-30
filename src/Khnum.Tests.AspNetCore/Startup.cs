using Khnum.PostgreSql;
using Khnum.Tests.Contracts;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Khnum.Tests.AspNetCore
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            var options = new PostgreSqlBusOptions
            {
                ConnectionString = "Server=127.0.0.1;Database=AmeaLegalAdmin;User Id=admin;Password=admin"
            };

            services.AddKhnumServiceBus<PostgreSqlBus, PostgreSqlPublisher, PostgreSqlBusOptions>(
                options,
                registry =>
                {
                    registry.Subscribe<SendTimeRequest, SendTimeRequestConsumer>("asp-net-core");
                });

            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1);
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, IApplicationLifetime lifetime)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseMvc();
            app.UseKhnumServiceBus();
        }
    }
}
