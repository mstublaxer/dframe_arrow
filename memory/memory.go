package memory

import (
	"sync"
)

// Releasable Resource
type Releasable interface {
	Release()
}

// Deallocators are meant to provide a way of function chaining while still
// adhering to proper management that might otherwise lead to leaks
type Deallocator interface {
	AddResourceToCleanup(resource Releasable)
	ReleaseResources()
}

type BaseDeallocator struct {
	arrs    []Releasable
	arrLock sync.Mutex // For synchronization
}

func NewBaseDeallocator() Deallocator {
	return &BaseDeallocator{}
}

func (d *BaseDeallocator) AddResourceToCleanup(resource Releasable) {
	if resource == nil {
		panic("Null Resource Not Allowed!")
	}

	if d.arrs == nil {
		d.arrs = make([]Releasable, 1)
	}

	d.arrLock.Lock()
	defer d.arrLock.Unlock()

	// To-Do, Should I Be Checking If Pointer Already Exists?
	d.arrs = append(d.arrs, resource)
}

// Releases all existing Releasables and Resets Deallocator by clearing
func (d *BaseDeallocator) ReleaseResources() {
	d.arrLock.Lock()
	defer d.arrLock.Unlock()

	for _, resource := range d.arrs {
		if resource != nil {
			resource.Release()
		}
	}
	d.arrs = nil // Clear the slice after release
}

// Wrapper around areas that must provide deallocators
func GuardDeallocator(dealloc *Deallocator) {
	if dealloc == nil {
		panic("Must Provide Deallocator!")
	}
}
