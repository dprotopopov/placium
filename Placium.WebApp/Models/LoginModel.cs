using System.ComponentModel.DataAnnotations;

namespace Placium.WebApp.Models
{
    public class LoginModel
    {
        [Required(ErrorMessage = "Не указан пароль")]
        [DataType(DataType.Password)]
        public string Password { get; set; }
    }
}
