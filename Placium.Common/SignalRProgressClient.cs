using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;

namespace Placium.Common
{
    public class SignalRProgressClient : IProgressClient
    {
        private readonly IHubContext<ProgressHub, IProgressHubClient> _progressHub;

        public SignalRProgressClient(IHubContext<ProgressHub, IProgressHubClient> progressHub)
        {
            _progressHub = progressHub;
        }

        public async Task Progress(float progress, string id, string session)
        {
            await _progressHub.Clients.All.Progress(progress, id, session);
        }

        public async Task Init(string id, string session)
        {
            await _progressHub.Clients.All.Init(id, session);
        }
    }
}