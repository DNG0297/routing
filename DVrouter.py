import json
from router import Router
from packet import Packet


class DVrouter(Router):
    """Distance vector routing protocol implementation."""

    def __init__(self, addr, heartbeat_time):
        Router.__init__(self, addr)  # Initialize base class - DO NOT REMOVE
        self.heartbeat_time = heartbeat_time
        self.last_time = 0
        # TODO: add your own class fields and initialization code here
        self.infinity = 16
        self.port_table = {}       # {port: {"peer": endpoint, "weight": cost}}
        self.min_costs = {addr: 0} # {destination: best_cost}
        self.next_hops = {}        # {destination: out_port}
        self.peer_vectors = {}     # {peer_addr: {destination: cost}}

    def _broadcast_vectors(self):
        """Send customized distance vector to each neighbor using Poisoned Reverse."""
        for out_port, info in self.port_table.items():
            peer_addr = info["peer"]
            advertised = {}
            for dest, total_cost in self.min_costs.items():
                # Poisoned reverse: advertise infinity if best path goes through this port
                if dest != peer_addr and self.next_hops.get(dest) == out_port:
                    advertised[dest] = self.infinity
                else:
                    advertised[dest] = total_cost

            payload = json.dumps({"dv": advertised})
            pkt = Packet(Packet.ROUTING, self.addr, peer_addr, payload)
            self.send(out_port, pkt)

    def _update_routing_state(self):
        """Recompute shortest paths via Bellman-Ford and notify peers if changed."""
        known_targets = {self.addr}
        for info in self.port_table.values():
            known_targets.add(info["peer"])
        for vec in self.peer_vectors.values():
            known_targets.update(vec.keys())

        updated_costs = {self.addr: 0}
        updated_hops = {}

        for target in known_targets:
            if target == self.addr:
                continue

            best_weight = self.infinity
            chosen_port = None

            for port, info in self.port_table.items():
                peer_addr = info["peer"]
                link_weight = info["weight"]

                if target == peer_addr:
                    candidate_weight = link_weight
                else:
                    peer_dist = self.peer_vectors.get(peer_addr, {}).get(target, self.infinity)
                    candidate_weight = link_weight + peer_dist

                if candidate_weight < best_weight:
                    best_weight = candidate_weight
                    chosen_port = port

            if best_weight < self.infinity and chosen_port is not None:
                updated_costs[target] = best_weight
                updated_hops[target] = chosen_port

        if updated_costs != self.min_costs or updated_hops != self.next_hops:
            self.min_costs = updated_costs
            self.next_hops = updated_hops
            self._broadcast_vectors()

    def handle_packet(self, port, packet):
        """Process incoming packet."""
        if packet.is_traceroute:
            out_port = self.next_hops.get(packet.dst_addr)
            if out_port is not None:
                self.send(out_port, packet)
        else:
            try:
                body = json.loads(packet.content)
                received_dv = body.get("dv", {})
                self.peer_vectors[packet.src_addr] = received_dv
                self._update_routing_state()
            except (ValueError, TypeError):
                pass

    def handle_new_link(self, port, endpoint, cost):
        """Handle new link."""
        self.port_table[port] = {"peer": endpoint, "weight": cost}
        if endpoint not in self.peer_vectors:
            self.peer_vectors[endpoint] = {}
        self._update_routing_state()

    def handle_remove_link(self, port):
        """Handle removed link."""
        removed = self.port_table.pop(port, None)
        if removed is not None:
            peer_addr = removed["peer"]
            # Check if there is still another parallel link to the same peer
            still_connected = any(
                item["peer"] == peer_addr for item in self.port_table.values()
            )
            if not still_connected:
                self.peer_vectors.pop(peer_addr, None)
            self._update_routing_state()

    def handle_time(self, time_ms):
        """Handle current time."""
        if time_ms - self.last_time >= self.heartbeat_time:
            self.last_time = time_ms
            self._broadcast_vectors()

    def __repr__(self):
        """Representation for debugging in the network visualizer."""
        lines = [f"DVrouter({self.addr})"]
        for dest, cost in sorted(self.min_costs.items()):
            port = self.next_hops.get(dest, "-")
            lines.append(f"  -> {dest}: cost={cost}, port={port}")
        return "\n".join(lines)
