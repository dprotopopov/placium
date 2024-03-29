﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ShellProgressBar;

namespace Placium.Common
{
    public class ShellProgressClient : IProgressClient
    {
        private const int MaxTicks = 10000;

        private static readonly ProgressBarOptions Options = new ProgressBarOptions
        {
            ForegroundColor = ConsoleColor.Yellow,
            ForegroundColorDone = ConsoleColor.DarkGreen,
            BackgroundColor = ConsoleColor.DarkGray,
            BackgroundCharacter = '\u2593'
        };

        private readonly Dictionary<string, ProgressBar> _dictionary = new Dictionary<string, ProgressBar>();

        public async Task Progress(float progress, string id, string session)
        {
            await Task.Run(() => _dictionary[id].Tick((int)(MaxTicks * progress / 100)));
        }

        public async Task Init(string id, string session)
        {
            await Task.Run(() => _dictionary.Add(id, new ProgressBar(MaxTicks, null, Options)));
        }

        public async Task Finalize(string id, string session)
        {
            await Task.Run(() =>
            {
                _dictionary[id].Tick(MaxTicks);
                _dictionary[id].Dispose();
                _dictionary.Remove(id);
            });
        }
    }
}