using System.Text;
using Route.Attributes;
using Route.LocalGeo;

namespace Placium.Route
{
    public class Route
    {
        /// <summary>
        /// Gets or sets the shape.
        /// </summary>
        public Coordinate[] Shape { get; set; }
        /// <summary>
        /// Gets or sets the attributes.
        /// </summary>
        public IAttributeCollection Attributes { get; set; }

        /// <summary>
        /// Gets or sets the stops.
        /// </summary>
        public Stop[] Stops { get; set; }

        /// <summary>
        /// Gets or sets the meta data.
        /// </summary>
        public Meta[] ShapeMeta { get; set; }

        /// <summary>
        /// Represents a stop.
        /// </summary>
        public class Stop
        {
            /// <summary>
            /// Gets or sets the shape index.
            /// </summary>
            public int Shape { get; set; }

            /// <summary>
            /// Gets or sets the coordinates.
            /// </summary>
            public Coordinate Coordinate { get; set; }

            /// <summary>
            /// Gets or sets the attributes.
            /// </summary>
            public IAttributeCollection Attributes { get; set; }

            /// <summary>
            /// Creates a clone of this object.
            /// </summary>
            public Stop Clone()
            {
                AttributeCollection attributes = null;
                if (this.Attributes != null)
                {
                    attributes = new AttributeCollection(this.Attributes);
                }
                return new Stop()
                {
                    Attributes = attributes,
                    Shape = this.Shape,
                    Coordinate = this.Coordinate
                };
            }

            /// <summary>
            /// The distance in meter.
            /// </summary>
            public float Distance
            {
                get
                {
                    if (this.Attributes == null)
                    {
                        return 0;
                    }
                    float value;
                    if (!this.Attributes.TryGetSingle("distance", out value))
                    {
                        return 0;
                    }
                    return value;
                }
                set
                {
                    if (this.Attributes == null)
                    {
                        this.Attributes = new AttributeCollection();
                    }
                    this.Attributes.SetSingle("distance", value);
                }
            }

            /// <summary>
            /// The time in seconds.
            /// </summary>
            public float Time
            {
                get
                {
                    if (this.Attributes == null)
                    {
                        return 0;
                    }
                    float value;
                    if (!this.Attributes.TryGetSingle("time", out value))
                    {
                        return 0;
                    }
                    return value;
                }
                set
                {
                    if (this.Attributes == null)
                    {
                        this.Attributes = new AttributeCollection();
                    }
                    this.Attributes.SetSingle("time", value);
                }
            }

            /// <summary>
            /// Returns a description of this stop.
            /// </summary>
            public override string ToString()
            {
                if (this.Attributes == null)
                {
                    return "@" + this.Coordinate.ToString();
                }
                return this.Attributes.ToString() + "@" + this.Coordinate.ToString();
            }
        }

        /// <summary>
        /// Represents meta-data about a part of this route.
        /// </summary>
        public class Meta
        {
            /// <summary>
            /// Gets or sets the shape index.
            /// </summary>
            public int Shape { get; set; }

            /// <summary>
            /// Gets or sets the attributes.
            /// </summary>
            public IAttributeCollection Attributes { get; set; }

            /// <summary>
            /// Gets or sets the relative direction flag of the attributes.
            /// </summary>
            public bool AttributesDirection { get; set; }

            /// <summary>
            /// Gets or sets the profile.
            /// </summary>
            public string Profile
            {
                get
                {
                    if (this.Attributes == null)
                    {
                        return string.Empty;
                    }
                    string value;
                    if (!this.Attributes.TryGetValue("profile", out value))
                    {
                        return string.Empty;
                    }
                    return value;
                }
                set
                {
                    if (this.Attributes == null)
                    {
                        this.Attributes = new AttributeCollection();
                    }
                    this.Attributes.AddOrReplace("profile", value);
                }
            }

            /// <summary>
            /// Creates a clone of this meta-object.
            /// </summary>
            /// <returns></returns>
            public Meta Clone()
            {
                AttributeCollection attributes = null;
                if (this.Attributes != null)
                {
                    attributes = new AttributeCollection(this.Attributes);
                }
                return new Meta()
                {
                    Attributes = attributes,
                    Shape = this.Shape
                };
            }

            /// <summary>
            /// The distance in meter.
            /// </summary>
            public float Distance
            {
                get
                {
                    if (this.Attributes == null)
                    {
                        return 0;
                    }
                    float value;
                    if (!this.Attributes.TryGetSingle("distance", out value))
                    {
                        return 0;
                    }
                    return value;
                }
                set
                {
                    if (this.Attributes == null)
                    {
                        this.Attributes = new AttributeCollection();
                    }
                    this.Attributes.SetSingle("distance", value);
                }
            }

            /// <summary>
            /// The time in seconds.
            /// </summary>
            public float Time
            {
                get
                {
                    if (this.Attributes == null)
                    {
                        return 0;
                    }
                    float value;
                    if (!this.Attributes.TryGetSingle("time", out value))
                    {
                        return 0;
                    }
                    return value;
                }
                set
                {
                    if (this.Attributes == null)
                    {
                        this.Attributes = new AttributeCollection();
                    }
                    this.Attributes.SetSingle("time", value);
                }
            }
        }

        /// <summary>
        /// Gets or sets the branches.
        /// </summary>
        public Branch[] Branches { get; set; }

        /// <summary>
        /// Represents a branch.
        /// </summary>
        public class Branch
        {
            /// <summary>
            /// Gets or sets the shape index.
            /// </summary>
            public int Shape { get; set; }

            /// <summary>
            /// Gets or sets the coordinates.
            /// </summary>
            public Coordinate Coordinate { get; set; }

            /// <summary>
            /// Gets or sets the attributes.
            /// </summary>
            public IAttributeCollection Attributes { get; set; }

            /// <summary>
            /// Gets or sets the relative direction flag of the attributes.
            /// </summary>
            public bool AttributesDirection { get; set; }

            /// <summary>
            /// Creates a clone of this object.
            /// </summary>
            public Branch Clone()
            {
                AttributeCollection attributes = null;
                if (this.Attributes != null)
                {
                    attributes = new AttributeCollection(this.Attributes);
                }
                return new Branch()
                {
                    Attributes = attributes,
                    Shape = this.Shape,
                    Coordinate = this.Coordinate
                };
            }
        }

        /// <summary>
        /// The distance in meter.
        /// </summary>
        public float TotalDistance
        {
            get
            {
                if (this.Attributes == null)
                {
                    return 0;
                }
                float value;
                if (!this.Attributes.TryGetSingle("distance", out value))
                {
                    return 0;
                }
                return value;
            }
            set
            {
                if (this.Attributes == null)
                {
                    this.Attributes = new AttributeCollection();
                }
                this.Attributes.SetSingle("distance", value);
            }
        }

        /// <summary>
        /// The time in seconds.
        /// </summary>
        public float TotalTime
        {
            get
            {
                if (this.Attributes == null)
                {
                    return 0;
                }
                float value;
                if (!this.Attributes.TryGetSingle("time", out value))
                {
                    return 0;
                }
                return value;
            }
            set
            {
                if (this.Attributes == null)
                {
                    this.Attributes = new AttributeCollection();
                }
                this.Attributes.SetSingle("time", value);
            }
        }

    }
}
