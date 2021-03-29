using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;

namespace Placium.Common
{
    public class ProgressHub : Hub
    {
        public async Task Progress(float progress, string id, string session)
        {
            await Clients.All.SendAsync("Progress", progress, id, session);
        }
        public async Task Init(string id, string session)
        {
            await Clients.All.SendAsync("Init", id, session);
        }
    }
}