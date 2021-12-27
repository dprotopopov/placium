namespace Placium.Route.Profiles
{
    /// <summary>
    /// Defines a constraint variable.
    /// </summary>
    public class Constraint
    {
        /// <summary>
        /// Creates a new constraint variable.
        /// </summary>
        public Constraint(string name, bool isMax, float defaultValue)
        {
            Name = name;
            DefaultValue = defaultValue;
            IsMax = isMax;
        }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the is max boolean.
        /// </summary>
        public bool IsMax { get; }

        /// <summary>
        /// Gets the default value.
        /// </summary>
        public float DefaultValue { get; }
    }
}