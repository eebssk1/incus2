package ip

import (
	"fmt"
	"net"

	"github.com/vishvananda/netlink"

	"golang.org/x/sys/unix"
)

// Addr represents arguments for address protocol manipulation.
type Addr struct {
	DevName string
	Address *net.IPNet
	Scope   string
	Family  Family
}

// Add adds new protocol address.
func (a *Addr) Add() error {
	scope, err := a.scopeNum()
	if err != nil {
		return err
	}

	err = netlink.AddrAdd(&netlink.GenericLink{
		LinkAttrs: netlink.LinkAttrs{
			Name: a.DevName,
		},
	}, &netlink.Addr{
		IPNet: a.Address,
		Scope: scope,
	})
	if err != nil {
		return fmt.Errorf("Failed to add address %q: %w", a.Address.String(), err)
	}

	return nil
}

func (a *Addr) scopeNum() (int, error) {
	var scope netlink.Scope
	switch a.Scope {
	case "global", "universe", "":
		scope = netlink.SCOPE_UNIVERSE
	case "site":
		scope = netlink.SCOPE_SITE
	case "link":
		scope = netlink.SCOPE_LINK
	case "host":
		scope = netlink.SCOPE_HOST
	case "nowhere":
		scope = netlink.SCOPE_NOWHERE
	default:
		return 0, fmt.Errorf("Unknown address scope %q", a.Scope)
	}

	return int(scope), nil
}

// Flush flushes protocol addresses.
func (a *Addr) Flush() error {
	link, err := linkByName(a.DevName)
	if err != nil {
		return err
	}

	addrs, err := netlink.AddrList(link, int(a.Family))
	if err != nil {
		return fmt.Errorf("Failed to get addresses for device %s: %w", a.DevName, err)
	}

	scope, err := a.scopeNum()
	if err != nil {
		return err
	}

	// NOTE: If this becomes a bottleneck, there appears to be support for batching those kind of changes within netlink.

	for _, addr := range addrs {
		if a.Scope != "" && scope != addr.Scope {
			continue
		}

		err := netlink.AddrDel(link, &addr)
		if err != nil {
			return fmt.Errorf("Failed to delete address %v: %w", addr, err)
		}
	}

	return nil
}

// Find and replace the default local route if CC need reset
func (a *Addr) SetRouteCC() error {
	link, err := netlink.LinkByName(a.DevName)
	if err != nil {
		return fmt.Errorf("Failed to change CC (Device): %w", err)
	}

	_, dstNet, err := net.ParseCIDR(a.Address.String())
	if err != nil {
		return fmt.Errorf("Failed to change CC (ParseCIDR): %w", err)
	}

	filter := &netlink.Route{
		LinkIndex: link.Attrs().Index,
		Dst:       dstNet,
		// Skip if it is changed externally during our process(which may remove kernel mark)
		Protocol: unix.RTPROT_KERNEL,
	}

	routes, err := netlink.RouteListFiltered(int(a.Family), filter, netlink.RT_FILTER_OIF|netlink.RT_FILTER_DST|netlink.RT_FILTER_PROTOCOL)
	if err != nil {
		return fmt.Errorf("Failed to change CC (FilterRouteList): %w", err)
	}

	// This is normal if the change called multiple times without reset.
	if len(routes) == 0 {
		return nil
	}

	route := routes[0]
	if int(a.Family) == unix.AF_INET6 {
		_ = netlink.RouteDel(&route)
		route.Priority = 1
	}
	route.Congctl = "highspeed"
	// Mark this is a modified one ?
	route.Protocol = unix.RTPROT_BOOT

	if int(a.Family) == unix.AF_INET6 {
		err = netlink.RouteAdd(&route)
	} else {
		err = netlink.RouteChange(&route)
	}
	if err != nil {
		return fmt.Errorf("Failed to change CC (Change): %w", err)
	}

	return nil
}
