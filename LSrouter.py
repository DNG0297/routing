import json
import heapq
from router import Router
from packet import Packet


class LSrouter(Router):
    """Link state routing protocol implementation."""

    def __init__(self, addr, heartbeat_time):
        Router.__init__(self, addr)  # Initialize base class - DO NOT REMOVE
        self.heartbeat_time = heartbeat_time
        self.last_time = 0
        # TODO: add your own class fields and initialization code here
        self.seq_id = 0
        self.local_ports = {}   # {port: (peer_addr, cost)}
        self.graph_db = {}      # {node_addr: {"seq": int, "edges": {peer_addr: cost}}}
        self.next_hops = {}     # {dest_addr: out_port}

    def _emit_local_lsp(self):
        """Increment sequence number, update local LSDB entry, and flood LSP to all neighbors."""
        self.seq_id += 1
        current_edges = {}
        for peer_addr, weight in self.local_ports.values():
            # Keep minimum cost if multiple parallel links exist to the same peer
            if peer_addr not in current_edges or weight < current_edges[peer_addr]:
                current_edges[peer_addr] = weight

        self.graph_db[self.addr] = {"seq": self.seq_id, "edges": current_edges}

        body = json.dumps({"seq": self.seq_id, "edges": current_edges})
        for out_port, (peer_addr, _) in self.local_ports.items():
            pkt = Packet(Packet.ROUTING, self.addr, peer_addr, body)
            self.send(out_port, pkt)

    def _flood_packet(self, incoming_port, packet):
        """Forward an unseen/newer LSP to all neighbors except the incoming port."""
        for out_port, (peer_addr, _) in self.local_ports.items():
            if out_port != incoming_port:
                fwd_pkt = Packet(Packet.ROUTING, packet.src_addr, peer_addr, packet.content)
                self.send(out_port, fwd_pkt)

    def _rebuild_forwarding_table(self):
        """Run Dijkstra's algorithm on the Link-State Database to update forwarding table."""
        distances = {self.addr: 0}
        first_step = {}
        min_heap = [(0, self.addr)]

        while min_heap:
            curr_dist, u = heapq.heappop(min_heap)
            if curr_dist > distances.get(u, float("inf")):
                continue

            u_edges = self.graph_db.get(u, {}).get("edges", {})
            for v, weight in u_edges.items():
                # If v is a router in graph_db, verify bidirectional consistency during link transitions
                if v in self.graph_db and u not in self.graph_db[v].get("edges", {}):
                    continue

                new_dist = curr_dist + weight
                if new_dist < distances.get(v, float("inf")):
                    distances[v] = new_dist
                    first_step[v] = v if u == self.addr else first_step[u]
                    heapq.heappush(min_heap, (new_dist, v))

        # Map each destination's first-hop peer to the best local port
        peer_to_best_port = {}
        peer_best_cost = {}
        for port, (peer_addr, weight) in self.local_ports.items():
            if peer_addr not in peer_best_cost or weight < peer_best_cost[peer_addr]:
                peer_best_cost[peer_addr] = weight
                peer_to_best_port[peer_addr] = port

        updated_hops = {}
        for dest, hop_peer in first_step.items():
            if hop_peer in peer_to_best_port:
                updated_hops[dest] = peer_to_best_port[hop_peer]

        self.next_hops = updated_hops

    def handle_packet(self, port, packet):
        """Process incoming packet."""
        if packet.is_traceroute:
            out_port = self.next_hops.get(packet.dst_addr)
            if out_port is not None:
                self.send(out_port, packet)
        else:
            try:
                data = json.loads(packet.content)
                origin = packet.src_addr
                incoming_seq = data.get("seq", -1)
                incoming_edges = data.get("edges", {})

                known_record = self.graph_db.get(origin)
                if known_record is None or incoming_seq > known_record["seq"]:
                    self.graph_db[origin] = {
                        "seq": incoming_seq,
                        "edges": incoming_edges,
                    }
                    self._flood_packet(port, packet)
                    self._rebuild_forwarding_table()
            except (ValueError, TypeError):
                pass

    def handle_new_link(self, port, endpoint, cost):
        """Handle new link."""
        self.local_ports[port] = (endpoint, cost)
        self._emit_local_lsp()
        self._rebuild_forwarding_table()

    def handle_remove_link(self, port):
        """Handle removed link."""
        if port in self.local_ports:
            del self.local_ports[port]
            self._emit_local_lsp()
            self._rebuild_forwarding_table()

    def handle_time(self, time_ms):
        """Handle current time."""
        if time_ms - self.last_time >= self.heartbeat_time:
            self.last_time = time_ms
            self._emit_local_lsp()

    def __repr__(self):
        """Representation for debugging in the network visualizer."""
        lines = [f"LSrouter({self.addr}, seq={self.seq_id})"]
        for dest, port in sorted(self.next_hops.items()):
            lines.append(f"  -> {dest}: port={port}")
        return "\n".join(lines)
