using System;
using System.Collections.Generic;
using Route.Algorithms.Collections;
using Route.Algorithms.Weights;
using Route.Data.Contracted.Edges;
using Route.Graphs.Directed;

namespace Route.Algorithms.Contracted.Witness
{
    public class NeighbourWitnessCalculator
    {
        protected readonly int _hopLimit;
        protected readonly int _maxSettles;

        public NeighbourWitnessCalculator(int hopLimit = int.MaxValue, int maxSettles = int.MaxValue)
        {
            _hopLimit = hopLimit;
            _maxSettles = maxSettles;
        } 

        protected Dictionary<long, float> forwardSettled = new Dictionary<long, float>();
        protected Dictionary<long, float> backwardSettled = new Dictionary<long, float>();
        //private List<Tuple<long, long, Shortcut<float>>> witnesses = new List<Tuple<long, long, Shortcut<float>>>();
        private List<Witness> witnesses = new List<Witness>();

        private struct Witness
        {
            public long Vertex1 { get; set; }

            public long Vertex2 { get; set; }

            //public long Witnessed { get; set; }

            public float Forward { get; set; }

            public float Backward { get; set; }
        }

        protected PathTree pathTree = new PathTree();
        protected BinaryHeap<long> pointerHeap = new BinaryHeap<long>();
        
        public void Run(DirectedGraph graph, DirectedGraph witnessGraph, long vertex, HashSet<long> dirty)
        {
            try
            {
                forwardSettled.Clear();
                backwardSettled.Clear();
                pathTree.Clear();
                pointerHeap.Clear();
                witnesses.Clear();

                var p = pathTree.AddSettledVertex(vertex, new WeightAndDir<float>()
                {
                    Direction = new Dir(true, true),
                    Weight = 0
                }, 0, Constants.NO_VERTEX);
                pointerHeap.Push(p, 0);

                // dequeue vertices until stopping conditions are reached.
                var enumerator = graph.GetEdgeEnumerator();

                // calculate max forward/backward weight.
                var forwardMax = 0f;
                var backwardMax = 0f;
                enumerator.MoveTo(vertex);
                var nextEnumerator = graph.GetEdgeEnumerator();
                while (enumerator.MoveNext())
                {
                    var nVertex1 = enumerator.Neighbour;

                    if (dirty != null &&
                        !dirty.Contains(nVertex1))
                    { // this is not a hop-2 to consider.
                        continue;
                    }

                    ContractedEdgeDataSerializer.Deserialize(enumerator.Data0,
                        out Dir dir1, out float weight1);
                    var p1 = pathTree.AddSettledVertex(nVertex1, weight1, dir1, 1, vertex);
                    pointerHeap.Push(p1, weight1);

                    nextEnumerator.MoveTo(enumerator.Neighbour);
                    while (nextEnumerator.MoveNext())
                    {
                        var nVertex2 = nextEnumerator.Neighbour;
                        if (nVertex2 == vertex)
                        { // no u-turns.
                            continue;
                        }

                        ContractedEdgeDataSerializer.Deserialize(nextEnumerator.Data0,
                            out Dir dir2, out float weight2);

                        dir2._val = (byte)(dir1._val & dir2._val);
                        if (dir2._val == 0)
                        {
                            continue;
                        }

                        var weight2Hops = weight1 + weight2;
                        var p2 = pathTree.AddSettledVertex(nVertex2, weight2Hops, dir2, 2, nVertex1);
                        pointerHeap.Push(p2, weight2Hops);

                        if (dir2.F && weight2Hops > forwardMax)
                        {
                            forwardMax = weight2Hops;
                        }
                        if (dir2.B && weight2Hops > backwardMax)
                        {
                            backwardMax = weight2Hops;
                        }
                    }
                }

                if (forwardMax == 0 &&
                    backwardMax == 0)
                {
                    return;
                }

                while (pointerHeap.Count > 0)
                {
                    var cPointer = pointerHeap.Pop();
                    pathTree.GetSettledVertex(cPointer, out long cVertex, out WeightAndDir<float> cWeight,
                        out long cHops, out long pVertex);

                    if (cHops == 2)
                    { // check if the search can stop or not.
                        var witness = new Shortcut<float>();
                        witness.Forward = float.MaxValue;
                        witness.Backward = float.MaxValue;
                        if (cWeight.Direction.F &&
                            forwardSettled.TryGetValue(cVertex, out float best) &&
                            best < cWeight.Weight)
                        { // this is a 2-hop and vertex was settled before, we have a witness!
                            witness.Forward = best;
                        }
                        if (cWeight.Direction.B &&
                            backwardSettled.TryGetValue(cVertex, out best) &&
                            best < cWeight.Weight)
                        { // this is a 2-hop and vertex was settled before, we have a witness!
                            witness.Backward = best;
                        }
                        if (witness.Backward != float.MaxValue ||
                            witness.Forward != float.MaxValue)
                        { // report witness here.
                            if (vertex != cVertex)
                            { // TODO: check this, how can they ever be the same?
                                witnesses.Add(new Witness()
                                {
                                    Vertex1 = vertex,
                                    Vertex2 = cVertex,
                                    Forward = witness.Forward,
                                    Backward = witness.Backward
                                });
                            }
                        }
                    }

                    if (forwardSettled.Count > _maxSettles ||
                        backwardSettled.Count > _maxSettles)
                    { // over settled count.
                        break;
                    }

                    if (cWeight.Weight > backwardMax &&
                        cWeight.Weight > forwardMax)
                    { // over max weights.
                        break;
                    }

                    if (forwardSettled.ContainsKey(cVertex) ||
                        cWeight.Weight > forwardMax)
                    {
                        cWeight.Direction = new Dir(false, cWeight.Direction.B);
                    }
                    if (backwardSettled.ContainsKey(cVertex) ||
                        cWeight.Weight > backwardMax)
                    {
                        cWeight.Direction = new Dir(cWeight.Direction.F, false);
                    }

                    var isRelevant = false;
                    if (cWeight.Direction.F)
                    {
                        forwardSettled.Add(cVertex, cWeight.Weight);
                        isRelevant = true;
                    }
                    if (cWeight.Direction.B)
                    {
                        backwardSettled.Add(cVertex, cWeight.Weight);
                        isRelevant = true;
                    }

                    if (!isRelevant)
                    { // not forward, not backwards.
                        continue;
                    }

                    cHops++;
                    if (cHops >= _hopLimit)
                    { // over hop limit, don't queue.
                        continue;
                    }

                    if (cHops == 1)
                    {
                        if (dirty == null)
                        { // all hops 1 are already queued.
                            continue;
                        }
                    }
                    else if (cHops == 2)
                    {
                        if (dirty == null ||
                            dirty.Contains(cVertex))
                        { // all these hops 2 are already queue.
                            continue;
                        }
                    }

                    enumerator.MoveTo(cVertex);
                    while (enumerator.MoveNext())
                    {
                        var nVertex = enumerator.Neighbour;

                        if (nVertex == pVertex)
                        { // no going back.
                            continue;
                        }

                        if (cHops == 1)
                        { // check if the neighbour is dirty.
                            if (dirty.Contains(nVertex))
                            { // skip dirty vertices, they are already in the queue.
                                continue;
                            }
                        }

                        Dir nDir;
                        float nWeight;
                        ContractedEdgeDataSerializer.Deserialize(enumerator.Data0,
                            out nDir, out nWeight);

                        nDir._val = (byte)(cWeight.Direction._val & nDir._val);
                        if (nDir._val == 0)
                        {
                            continue;
                        }

                        nWeight = nWeight + cWeight.Weight;

                        if (nDir.F && nWeight > forwardMax)
                        {
                            nDir = new Dir(false, nDir.B);
                            if (nDir._val == 0)
                            {
                                continue;
                            }
                        }
                        if (nDir.B && nWeight > backwardMax)
                        {
                            nDir = new Dir(nDir.F, false);
                            if (nDir._val == 0)
                            {
                                continue;
                            }
                        }

                        var nPoiner = pathTree.AddSettledVertex(nVertex, nWeight, nDir, cHops, cVertex);
                        pointerHeap.Push(nPoiner, nWeight);
                    }
                }

                if (witnesses.Count > 0)
                {
                    lock (witnessGraph)
                    {
                        foreach (var witness in witnesses)
                        {
                            //witnessGraph.AddOrUpdateEdge(witness.Item1, witness.Item2, witness.Item3.Forward,
                            //        witness.Item3.Backward);
                            witnessGraph.AddOrUpdateEdge(witness.Vertex1, witness.Vertex2,
                                witness.Forward, witness.Backward);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}