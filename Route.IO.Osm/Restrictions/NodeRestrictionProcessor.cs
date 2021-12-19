using System;
using System.Collections.Generic;
using OsmSharp;
using Route.IO.Osm.Streams;

namespace Route.IO.Osm.Restrictions
{
    /// <summary>
    /// An osm-data processor to process node based restrictions.
    /// </summary>
    /// <remarks>
    /// This is here to ensure backwards compat with non-lua based profiles nothing else.
    /// </remarks>
    public class NodeRestrictionProcessor : ITwoPassProcessor
    {
        private readonly Action<string, List<long>> _foundRestriction; // restriction found action.
        private readonly Func<Node, long> _markCore; // marks the node as core.

        /// <summary>
        /// Creates a new restriction processor.
        /// </summary>
        public NodeRestrictionProcessor(Func<Node, long> markCore, Action<string, List<long>> foundRestriction)
        {
            _foundRestriction = foundRestriction;
            _markCore = markCore;
        }

        public void FirstPass(Node node)
        {
            if (node.Tags != null &&
                (node.Tags.Contains("barrier", "bollard") ||
                 node.Tags.Contains("barrier", "fence") ||
                 node.Tags.Contains("barrier", "gate")))
            {
                _markCore(node);
                var r = new List<long>() {node.Id.Value};
                _foundRestriction("motorcar", r);
            }
        }

        /// <summary>
        /// Processes the given way in the first pass.
        /// </summary>
        public void FirstPass(Way way)
        {

        }

        /// <summary>
        /// Processes the given relation in the first pass.
        /// </summary>
        public bool FirstPass(Relation relation)
        {
            return false;
        }

        /// <summary>
        /// Processes the given node in the second pass.
        /// </summary>
        public void SecondPass(Node node)
        {
        }

        /// <summary>
        /// Processes the given way in the second pass.
        /// </summary>
        public void SecondPass(Way way)
        {
            
        }

        /// <summary>
        /// Processes the given relation in the second pass.
        /// </summary>
        public void SecondPass(Relation relation)
        {
            
        }
    }
}