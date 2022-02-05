using Route.Attributes;
using Route.LocalGeo;

namespace Placium.Route;

public class Route
{
    /// <summary>
    ///     Gets or sets the shape.
    /// </summary>
    public Coordinate[] Shape { get; set; }

    /// <summary>
    ///     Gets or sets the attributes.
    /// </summary>
    public IAttributeCollection Attributes { get; set; }

    /// <summary>
    ///     Gets or sets the stops.
    /// </summary>
    public Stop[] Stops { get; set; }

    /// <summary>
    ///     Gets or sets the meta data.
    /// </summary>
    public Meta[] ShapeMeta { get; set; }

    /// <summary>
    ///     Gets or sets the branches.
    /// </summary>
    public Branch[] Branches { get; set; }

    /// <summary>
    ///     The distance in meter.
    /// </summary>
    public float TotalDistance
    {
        get
        {
            if (Attributes == null) return 0;
            if (!Attributes.TryGetSingle("distance", out var value)) return 0;
            return value;
        }
        set
        {
            if (Attributes == null) Attributes = new AttributeCollection();
            Attributes.SetSingle("distance", value);
        }
    }

    /// <summary>
    ///     The time in seconds.
    /// </summary>
    public float TotalTime
    {
        get
        {
            if (Attributes == null) return 0;
            if (!Attributes.TryGetSingle("time", out var value)) return 0;
            return value;
        }
        set
        {
            if (Attributes == null) Attributes = new AttributeCollection();
            Attributes.SetSingle("time", value);
        }
    }

    /// <summary>
    ///     Represents a stop.
    /// </summary>
    public class Stop
    {
        /// <summary>
        ///     Gets or sets the shape index.
        /// </summary>
        public int Shape { get; set; }

        /// <summary>
        ///     Gets or sets the coordinates.
        /// </summary>
        public Coordinate Coordinate { get; set; }

        /// <summary>
        ///     Gets or sets the attributes.
        /// </summary>
        public IAttributeCollection Attributes { get; set; }

        /// <summary>
        ///     The distance in meter.
        /// </summary>
        public float Distance
        {
            get
            {
                if (Attributes == null) return 0;

                if (!Attributes.TryGetSingle("distance", out var value)) return 0;
                return value;
            }
            set
            {
                if (Attributes == null) Attributes = new AttributeCollection();
                Attributes.SetSingle("distance", value);
            }
        }

        /// <summary>
        ///     The time in seconds.
        /// </summary>
        public float Time
        {
            get
            {
                if (Attributes == null) return 0;

                if (!Attributes.TryGetSingle("time", out var value)) return 0;
                return value;
            }
            set
            {
                if (Attributes == null) Attributes = new AttributeCollection();
                Attributes.SetSingle("time", value);
            }
        }

        /// <summary>
        ///     Creates a clone of this object.
        /// </summary>
        public Stop Clone()
        {
            AttributeCollection attributes = null;
            if (Attributes != null) attributes = new AttributeCollection(Attributes);
            return new Stop
            {
                Attributes = attributes,
                Shape = Shape,
                Coordinate = Coordinate
            };
        }

        /// <summary>
        ///     Returns a description of this stop.
        /// </summary>
        public override string ToString()
        {
            if (Attributes == null) return "@" + Coordinate;
            return Attributes + "@" + Coordinate;
        }
    }

    /// <summary>
    ///     Represents meta-data about a part of this route.
    /// </summary>
    public class Meta
    {
        /// <summary>
        ///     Gets or sets the shape index.
        /// </summary>
        public int Shape { get; set; }

        /// <summary>
        ///     Gets or sets the attributes.
        /// </summary>
        public IAttributeCollection Attributes { get; set; }

        /// <summary>
        ///     Gets or sets the relative direction flag of the attributes.
        /// </summary>
        public bool AttributesDirection { get; set; }

        /// <summary>
        ///     Gets or sets the profile.
        /// </summary>
        public string Profile
        {
            get
            {
                if (Attributes == null) return string.Empty;

                if (!Attributes.TryGetValue("profile", out var value)) return string.Empty;
                return value;
            }
            set
            {
                if (Attributes == null) Attributes = new AttributeCollection();
                Attributes.AddOrReplace("profile", value);
            }
        }

        /// <summary>
        ///     The distance in meter.
        /// </summary>
        public float Distance
        {
            get
            {
                if (Attributes == null) return 0;

                if (!Attributes.TryGetSingle("distance", out var value)) return 0;
                return value;
            }
            set
            {
                if (Attributes == null) Attributes = new AttributeCollection();
                Attributes.SetSingle("distance", value);
            }
        }

        /// <summary>
        ///     The time in seconds.
        /// </summary>
        public float Time
        {
            get
            {
                if (Attributes == null) return 0;

                if (!Attributes.TryGetSingle("time", out var value)) return 0;
                return value;
            }
            set
            {
                if (Attributes == null) Attributes = new AttributeCollection();
                Attributes.SetSingle("time", value);
            }
        }

        /// <summary>
        ///     Creates a clone of this meta-object.
        /// </summary>
        /// <returns></returns>
        public Meta Clone()
        {
            AttributeCollection attributes = null;
            if (Attributes != null) attributes = new AttributeCollection(Attributes);
            return new Meta
            {
                Attributes = attributes,
                Shape = Shape
            };
        }
    }

    /// <summary>
    ///     Represents a branch.
    /// </summary>
    public class Branch
    {
        /// <summary>
        ///     Gets or sets the shape index.
        /// </summary>
        public int Shape { get; set; }

        /// <summary>
        ///     Gets or sets the coordinates.
        /// </summary>
        public Coordinate Coordinate { get; set; }

        /// <summary>
        ///     Gets or sets the attributes.
        /// </summary>
        public IAttributeCollection Attributes { get; set; }

        /// <summary>
        ///     Gets or sets the relative direction flag of the attributes.
        /// </summary>
        public bool AttributesDirection { get; set; }

        /// <summary>
        ///     Creates a clone of this object.
        /// </summary>
        public Branch Clone()
        {
            AttributeCollection attributes = null;
            if (Attributes != null) attributes = new AttributeCollection(Attributes);
            return new Branch
            {
                Attributes = attributes,
                Shape = Shape,
                Coordinate = Coordinate
            };
        }
    }
}