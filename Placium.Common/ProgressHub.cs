using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;

namespace Placium.Common
{
    public class ProgressHub : Hub
    {
        public async Task ProgressAsync(float progress, string id, string session)
        {
            await Clients.All.SendAsync("Progress", progress, id, session);
        }

        public async Task InitAsync(string id, string session)
        {
            await Clients.All.SendAsync("Init", id, session);
        }

        public async Task CompleteAsync(string session)
        {
            await Clients.All.SendAsync("Complete", session);
        }

        public async Task ErrorAsync(string message,string session)
        {
            await Clients.All.SendAsync("Error", message, session);
        }
    }
}
