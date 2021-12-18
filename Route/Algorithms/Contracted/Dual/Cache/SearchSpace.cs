using System;
using System.Collections.Generic;
using Route.Algorithms.Collections;

namespace Route.Algorithms.Contracted.Dual.Cache
{
    /// <summary>
    /// A search space.
    /// </summary>
    public class SearchSpace<T>
    {
        /// <summary>
        /// The visit tree.
        /// </summary>
        public PathTree Tree { get; set; }
        
        /// <summary>
        /// Gets or sets the visits per vertex.
        /// </summary>
        public Dictionary<long, Tuple<long, T>> Visits { get; set; }    
        
        /// <summary>
        /// Gets or sets the visits in one set.
        /// </summary>
        public HashSet<long> VisitSet { get; set; }      
    }
}