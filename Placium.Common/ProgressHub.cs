using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;

namespace Placium.Common
{
    public class ProgressHub : Hub
    {
        public async Task Progress(float progress, string session)
        {
            await Clients.All.SendAsync("Progress", progress, session);
        }
    }
}