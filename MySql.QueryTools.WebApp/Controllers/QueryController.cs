using System;
using System.Collections;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using MySql.QueryTools.WebApp.Models;

namespace MySql.QueryTools.WebApp.Controllers
{
    public class QueryController : Controller
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<QueryController> _logger;

        public QueryController(IConfiguration configuration, ILogger<QueryController> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        [HttpGet]
        public async Task<IActionResult> Index(string q)
        {
            if (string.IsNullOrEmpty(q)) return await Task.FromResult(View());

            _logger.LogTrace($"Execute query '{q}'");

            var model = new QueryViewModel
            {
                Query = q
            };

            try
            {
                model.Items = new ArrayList();
                model.Headers = new ArrayList();

                await using var connection =
                    new MySqlConnection(_configuration.GetConnectionString("SphinxConnection"));
                await connection.OpenAsync();
                await using (var command = new MySqlCommand(model.Query, connection))
                {
                    await using var reader = command.ExecuteReader();
                    var first = true;
                    while (reader.Read())
                    {
                        var row = new ArrayList();
                        for (var i = 0; i < reader.FieldCount; i++) row.Add(reader.GetValue(i));
                        if (first)
                            for (var i = 0; i < reader.FieldCount; i++)
                                model.Headers.Add(reader.GetName(i));
                        model.Items.Add(row);
                        first = false;
                    }
                }

                await connection.CloseAsync();
                _logger.LogInformation($"Success execute query '{q}'. Returns {model.Items.Count} records.");
            }
            catch (Exception ex)
            {
                model.Error = ex.Message;
                _logger.LogError($"Error while execute query '{q}'. {ex.Message}");
            }

            return await Task.FromResult(View(model));
        }
    }
}