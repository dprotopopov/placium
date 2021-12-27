namespace Placium.Route.Profiles
{
    /// <summary>
    /// Abstract definition of a profile instance.
    /// </summary>
    public interface IProfileInstance
    {
        /// <summary>
        /// Gets the profile.
        /// </summary>
        Profile Profile
        {
            get;
        }

        /// <summary>
        /// Gets the constraint boundaries.
        /// </summary>
        float[] Constraints
        {
            get;
        }
    }

    class ProfileInstance : IProfileInstance
    {
        private readonly Profile _profile;
        private readonly float[] _constraints;

        /// <summary>
        /// Creates a new profile instance.
        /// </summary>
        public ProfileInstance(Profile profile, float[] constraints)
        {
            _profile = profile;
            _constraints = constraints;
        }

        /// <summary>
        /// Gets the constraints.
        /// </summary>
        public float[] Constraints
        {
            get
            {
                return _constraints;
            }
        }

        /// <summary>
        /// Gets the profile.
        /// </summary>
        public Profile Profile
        {
            get
            {
                return _profile;
            }
        }
    }
}