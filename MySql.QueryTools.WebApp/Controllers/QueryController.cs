using System;
using System.Collections;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using MySql.QueryTools.WebApp.Models;

namespace MySql.QueryTools.WebApp.Controllers
{
    public class QueryController : Controller
    {
        private readonly IConfiguration _configuration;

        public QueryController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task<IActionResult> Index()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Index(QueryViewModel model)
        {
            try
            {
                model.Items = new ArrayList();
                model.Headers = new ArrayList();

                await using var connection =
                    new MySqlConnection(_configuration.GetConnectionString("SphinxConnection"));
                connection.Open();
                using (var command = new MySqlCommand(model.Query, connection))
                {
                    using var reader = command.ExecuteReader();
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

                connection.Close();
            }
            catch (Exception ex)
            {
                model.Error = ex.Message;
            }

            return View(model);
        }
    }
}