package kvstore

// TODO:
// $ echo '{"Items": {"key": "value2"}}' | nomad operator api '/v1/var/test/one'
//   ^ works
// $ echo '{"Items": {"key": "value2"}}' | nomad operator api '/v1/var/test////one'
//   ^ pretends to work
//
// oh dear, does not like periods (like ip addresses):
// $ echo '{"Items": {"key": "value2"}}' | nomad operator api '/v1/var/test/one.two'
// RPC Error:: 400,invalid path "test/one.two"
//

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/cilium/cilium/pkg/time"
	nomadAPI "github.com/hashicorp/nomad/api"
)

const (
	nomadName = "nomad"
)

func init() {
	registerBackend(nomadName, newNomadModule())
}

var _ backendModule = &nomadModule{}

type nomadModule struct {
	// TODO: do I need to protect this?
	config map[string]string
	opts   *ExtraOptions
}

func newNomadModule() *nomadModule {
	log.Println("HI HELLO: newNomadModule")
	return &nomadModule{
		config: make(map[string]string),
	}
}

func (n *nomadModule) getName() string {
	return nomadName
}

func (n *nomadModule) setConfig(opts map[string]string) error {
	n.config = opts
	return nil
}

func (n *nomadModule) setExtraConfig(opts *ExtraOptions) error {
	n.opts = opts
	return nil
}

func (n *nomadModule) setConfigDummy() {
	return
}

func (n *nomadModule) getConfig() map[string]string {
	return n.config
}

func (n *nomadModule) newClient(ctx context.Context, opts *ExtraOptions) (BackendOperations, chan error) {
	errChan := make(chan error, 2)
	client, err := nomadAPI.NewClient(nomadAPI.DefaultConfig())
	if err != nil {
		errChan <- fmt.Errorf("newClient NewClient: %w", err)
	}
	backend := &nomadBackend{
		nomad:  client,
		config: n.getConfig(),
		opts:   opts,
		//root:   "cilium-state-zzzzzzzzzzzzzzzzzzzzz", // TODO: configurable?  beware max URL path length: nomad validVariablePath
		//root:  "cilium/" + opts.ClusterName, // note: ClusterName can be empty
		//root: "",
	}
	if _, err = backend.Status(); err != nil {
		errChan <- fmt.Errorf("newClient Status: %w", err)
	}
	close(errChan)
	return backend, errChan
}

func (n *nomadModule) createInstance() backendModule {
	return newNomadModule()
}

var _ BackendOperations = &nomadBackend{}

type nomadBackend struct {
	nomad  *nomadAPI.Client
	config map[string]string
	opts   *ExtraOptions
	root   string // nomad variable root -- ALL the k/v will go in here // TODO: should it?

	statusErrs <-chan error // TODO: ???
}

func nomadQO(ctx context.Context) (qo *nomadAPI.QueryOptions) {
	// TODO: not default namespace...
	return qo.WithContext(ctx)
}

func nomadWO(ctx context.Context) (wo *nomadAPI.WriteOptions) {
	return wo.WithContext(ctx)
}

func (n *nomadBackend) path(key string) string {
	key = strings.Replace(key, ".", "_", -1)
	//return key
	return n.root + "/" + key
}

func (n *nomadBackend) Connected(ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	go func() {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				ch <- fmt.Errorf("Connected ctx done: %w", ctx.Err())
				return
			default:
			}
			_, err := n.Status()
			if err == nil {
				fmt.Println("💚 connected to Nomad")
				return
			}
			ch <- err
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return ch
}

func (n *nomadBackend) Disconnected() <-chan struct{} { // TODO
	ch := make(chan struct{}, 1)
	return ch
}

func (n *nomadBackend) Status() (string, error) {
	return n.nomad.Status().Leader() // TODO: better status symbol?  also... can panic..?
}

func (n *nomadBackend) StatusCheckErrors() <-chan error {
	if n.statusErrs != nil {
		return n.statusErrs
	}
	ch := make(chan error, 128) // TODO: ??
	n.statusErrs = ch
	go func() {
		allConnected := true // TODO: not always true?
		ticker := time.NewTicker(n.opts.StatusCheckInterval(allConnected))
		defer ticker.Stop()
		for {
			if _, err := n.Status(); err != nil {
				ch <- fmt.Errorf("StatusCheckErrors: %w", err)
			}
			select {
			// curious there's no context for this method...
			// i think because etcd just returns a channel and does no work.
			case <-ticker.C:
			}
		}
	}()
	return ch
}

func (n *nomadBackend) LockPath(ctx context.Context, path string) (KVLocker, error) {
	locker := &nomadLocker{
		nomad:     n.nomad,
		namespace: "default", // TODO
		path:      n.path(path),
	}
	err := locker.Lock(ctx)
	return locker, err
}

// TODO: move me

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), nomadAPI.ErrVariablePathNotFound.Error())
}

var _ KVLocker = &nomadLocker{}

type nomadLocker struct {
	nomad     *nomadAPI.Client
	namespace string
	path      string
	variable  *nomadAPI.Variable
}

func (nl *nomadLocker) Lock(ctx context.Context) error {
	if nl.isLocked() {
		return nil
	}
	v := &nomadAPI.Variable{
		Namespace: nl.namespace, // TODO
		Path:      nl.path,
	}
	v.Lock = &nomadAPI.VariableLock{
		TTL:       "0", // TODO: does this work? I probably need something to maintain a lease...
		LockDelay: "0",
	}
	fmt.Println("ACQUIRE LOCK ON:", nl.path)
	v, _, err := nl.nomad.Variables().AcquireLock(v, nomadWO(ctx))
	if err != nil {
		return err
	}
	nl.variable = v
	return nil
}

func (nl *nomadLocker) isLocked() bool {
	if nl.variable == nil {
		fmt.Printf("not locked, no variable: %s\n", nl.path)
		return false
	}
	if nl.variable.LockID() == "" {
		fmt.Printf("not locked; no lock id: %s\n", nl.path)
		return false
	}
	return true
}

func (nl *nomadLocker) Unlock(ctx context.Context) error {
	if !nl.isLocked() {
		return nil
	}
	// TODO: nil variable here makes a panic
	fmt.Println("RELEASE LOCK ON:", nl.path)
	v, _, err := nl.nomad.Variables().ReleaseLock(nl.variable, nomadWO(ctx))
	if err != nil {
		return fmt.Errorf("could not Unlock: %w", err)
	}
	nl.variable = v
	// cilium doesn't store any contents in the locked var,
	// so delete it too for tidiness.
	_, err = nl.nomad.Variables().Delete(nl.path, nomadWO(ctx))
	if err != nil {
		fmt.Printf("warning: could not delete var: %s", err)
	}
	return nil
}

func (nl *nomadLocker) Comparator() interface{} {
	return nil // TODO...?  etcd does, but consul does not
}

func (n *nomadBackend) Get(ctx context.Context, key string) ([]byte, error) {
	items, _, err := n.nomad.Variables().GetVariableItems(n.path(key), nomadQO(ctx))
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	v, ok := items["data"]
	if !ok {
		return nil, nil
	}
	return []byte(v), nil
}

func (n *nomadBackend) checkLocked(lock KVLocker) error {
	l, ok := lock.(*Lock)
	if !ok {
		return errors.New("not a kvstore.Lock")
	}
	ll, ok := l.kvLock.(*nomadLocker)
	if !ok {
		return errors.New("lock not a nomadLocker")
	}
	if !ll.isLocked() {
		return errors.New("lock not held")
	}
	return nil
}

func (n *nomadBackend) GetIfLocked(ctx context.Context, key string, lock KVLocker) ([]byte, error) {
	if err := n.checkLocked(lock); err != nil {
		return nil, err
	}
	return n.Get(ctx, key)
}

func (n *nomadBackend) GetPrefix(ctx context.Context, prefix string) (string, []byte, error) {
	vars, _, err := n.nomad.Variables().List(nomadQO(ctx))
	if err != nil {
		return "", nil, err
	}
	for _, v := range vars {
		if strings.HasPrefix(v.Path, prefix) {
			vv, err := n.Get(ctx, v.Path)
			return v.Path, vv, err
		}
	}
	return "", nil, fmt.Errorf("no path matching prefix '%s'", prefix)
}

func (n *nomadBackend) GetPrefixIfLocked(ctx context.Context, prefix string, lock KVLocker) (string, []byte, error) {
	if err := n.checkLocked(lock); err != nil {
		return "", nil, err
	}
	return n.GetPrefix(ctx, prefix)
}

func (n *nomadBackend) Set(ctx context.Context, key string, value []byte) error {
	v := &nomadAPI.Variable{
		Namespace: "default", // TODO: configurable namespace
		Path:      n.path(key),
		Items: map[string]string{
			"data": string(value), // TODO: always "data" item?
		},
	}
	_, _, err := n.nomad.Variables().Create(v, nomadWO(ctx))
	return err
}

func (n *nomadBackend) Delete(ctx context.Context, key string) error {
	_, err := n.nomad.Variables().Delete(n.path(key), nomadWO(ctx))
	return err
}

func (n *nomadBackend) DeleteIfLocked(ctx context.Context, key string, lock KVLocker) error {
	if err := n.checkLocked(lock); err != nil {
		return err
	}
	return n.Delete(ctx, key)
}

func (n *nomadBackend) DeletePrefix(ctx context.Context, path string) error {
	p, _, err := n.GetPrefix(ctx, path)
	if err != nil {
		return err // TODO: is it an error if it doesn't already exist?
	}
	return n.Delete(ctx, p)
}

func (n *nomadBackend) Update(ctx context.Context, key string, value []byte, lease bool) error {
	// TODO: lease param?

	v := &nomadAPI.Variable{
		Namespace: "default", // TODO
		Path:      n.path(key),
		Items: map[string]string{
			"data": string(value),
		},
	}
	ll := log.WithFields(map[string]any{
		"method": "Update",
		"key":    key,
		"path":   v.Path,
		"val":    v.Items["data"],
	})
	_, _, err := n.nomad.Variables().Update(v, nomadWO(ctx))
	if isNotFound(err) {
		ll.Println("backend Update: not found, so creating")
		_, _, err = n.nomad.Variables().Create(v, nomadWO(ctx))
	}
	ll.WithError(err).Println("HI WOW NICE")
	return err
}

func (n *nomadBackend) UpdateIfLocked(ctx context.Context, key string, value []byte, lease bool, lock KVLocker) error {
	if err := n.checkLocked(lock); err != nil {
		return err
	}
	return n.Update(ctx, key, value, lease)
}

func (n *nomadBackend) UpdateIfDifferent(ctx context.Context, key string, value []byte, lease bool) (bool, error) {
	v, err := n.Get(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if bytes.Equal(v, value) {
		return false, nil
	}
	return true, n.Update(ctx, key, value, lease)
}

func (n *nomadBackend) UpdateIfDifferentIfLocked(ctx context.Context, key string, value []byte, lease bool, lock KVLocker) (bool, error) {
	if err := n.checkLocked(lock); err != nil {
		return false, err
	}
	return n.UpdateIfDifferent(ctx, key, value, lease)
}

func (n *nomadBackend) create(ctx context.Context, key string, value []byte) error {
	v := &nomadAPI.Variable{
		Namespace: "default", // TODO
		Path:      n.path(key),
		Items: map[string]string{
			"data": string(value),
		},
	}
	_, _, err := n.nomad.Variables().Create(v, nomadWO(ctx))
	return err
}

func (n *nomadBackend) CreateOnly(ctx context.Context, key string, value []byte, lease bool) (bool, error) {
	// TODO: `lease` param?
	bts, err := n.Get(ctx, key)
	if err != nil && !isNotFound(err) {
		return false, err
	}
	if bts != nil {
		return false, fmt.Errorf("CreateOnly key already exists: %s", key)
	}
	if err := n.create(ctx, key, value); err != nil {
		return false, err
	}
	return true, nil
}

func (n *nomadBackend) CreateOnlyIfLocked(ctx context.Context, key string, value []byte, lease bool, lock KVLocker) (bool, error) {
	if err := n.checkLocked(lock); err != nil {
		return false, err
	}
	return n.CreateOnly(ctx, key, value, lease)
}

func (n *nomadBackend) CreateIfExists(ctx context.Context, condKey, key string, value []byte, lease bool) error {
	_, err := n.Get(ctx, condKey)
	if err != nil {
		return fmt.Errorf("CreateIfExists: %w", err)
	}
	return n.create(ctx, key, value)
}

func (n *nomadBackend) ListPrefix(ctx context.Context, prefix string) (KeyValuePairs, error) {
	kv := make(map[string]Value)
	vars, meta, err := n.nomad.Variables().List(nomadQO(ctx))
	if err != nil {
		return nil, err
	}
	var v nomadAPI.VariableItems
	for _, vm := range vars {
		if !strings.HasPrefix(vm.Path, prefix) {
			continue
		}
		qo := nomadQO(ctx)
		qo.WaitIndex = meta.LastIndex - 1 // want similar currentness
		v, meta, err = n.nomad.Variables().GetVariableItems(vm.Path, qo)
		if err != nil {
			return kv, err
		}
		kv[vm.Path] = Value{
			Data:        []byte(v["data"]),
			ModRevision: meta.LastIndex,
			//LeaseID: ...,
			//SessionID: ...,
		}
	}
	return kv, nil
}

func (n *nomadBackend) ListPrefixIfLocked(ctx context.Context, prefix string, lock KVLocker) (KeyValuePairs, error) {
	if err := n.checkLocked(lock); err != nil {
		return nil, err
	}
	return n.ListPrefix(ctx, prefix)
}

func (n *nomadBackend) Watch(ctx context.Context, w *Watcher) {
	// TODO: Watch() could have only one Variables.List query to rule them all?
	getVar := func(path string) (string, bool) {
		items, _, err := n.nomad.Variables().GetVariableItems(path, nomadQO(ctx))
		if err != nil {
			log.WithError(err).WithField("path", path).Println("failed to get var")
			return "", false
		}
		v, ok := items["data"]
		return v, ok
	}
	listSignalSent := false
	go func() {
		defer w.Stop()
		qo := nomadQO(ctx)
		stuff := map[string]string{}
		for {
			// TODO: rate limiting
			select {
			case <-ctx.Done():
				log.Println("ctx.Done")
				return
			case <-w.stopWatch:
				log.Println("w.stopWatch")
				return
			default:
			}

			log.Println("watching prefix:", w.Prefix)
			vars, meta, err := n.nomad.Variables().List(qo) // TODO: PrefixList()
			if err != nil {
				log.WithError(err).Println("sux getting vars")
				time.Sleep(time.Second) // TODO: better rate limit
				continue
			}

			newStuff := map[string]bool{}
			for _, v := range vars {
				ll := log.WithFields(map[string]any{
					"path":   v.Path,
					"prefix": w.Prefix},
				)
				if !strings.HasPrefix(v.Path, w.Prefix) {
					//ll.Println("skipping because wrong prefix")
					continue
				}

				newStuff[v.Path] = true

				// only care about recently changed vars
				if v.ModifyIndex < qo.WaitIndex {
					//ll.WithFields(map[string]any{
					//	"v.mod":   v.ModifyIndex,
					//	"qo.wait": qo.WaitIndex,
					//}).Println("ignoring old news")
					continue
				}

				// get the real value
				val, ok := getVar(v.Path)
				if !ok {
					continue
				}

				// see if we already have it saved
				p, ok := stuff[v.Path]
				if !ok {
					// nope, add it
					ll.WithField("val", val).Println("got new val")
					w.Events <- KeyValueEvent{
						Key:   v.Path,
						Value: []byte(val),
						Typ:   EventTypeCreate,
					}
				} else {
					// yep, so it must be modified
					ll.WithField("val", val).WithField("prev", p).
						Println("got updated val")
					w.Events <- KeyValueEvent{
						Key:   v.Path,
						Value: []byte(val),
						Typ:   EventTypeModify,
					}
				}
				stuff[v.Path] = val
			}
			qo.WaitIndex = meta.LastIndex

			for path := range stuff {
				if !newStuff[path] {
					log.WithField("path", path).Println("delete old path")
					delete(stuff, path)
					w.Events <- KeyValueEvent{
						Key:   path,
						Value: nil,
						Typ:   EventTypeDelete,
					}
				}
			}

			// Only send the list signal once
			if !listSignalSent {
				w.Events <- KeyValueEvent{Typ: EventTypeListDone}
				listSignalSent = true
			}
		}
		//	items, meta, err := n.nomad.Variables().GetVariableItems(prefix, qo)
		//	qo.WaitIndex = meta.LastIndex
		//	if err != nil {
		//		if isNotFound(err) {
		//			w.Events <- KeyValueEvent{
		//				Key:   prefix,
		//				Value: nil,
		//				Typ:   EventTypeDelete,
		//			}
		//			continue
		//		}
		//		log.Println("sux 2 suk:", err)
		//		continue
		//	}
		//	v, ok := items["key"]
		//	if !ok {
		//		log.Printf("no dang key in items: %+v", items)
		//		continue
		//	}
		//	w.Events <- KeyValueEvent{
		//		Key:   prefix,
		//		Value: []byte(v),
		//		Typ:   EventTypeModify,
		//	}
		//}
	}()
}

func (n *nomadBackend) Close(ctx context.Context) {
	panic("implement me")
}

func (n *nomadBackend) GetCapabilities() Capabilities {
	//return Capabilities(CapabilityCreateIfExists)
	return Capabilities(0)
}

func (n *nomadBackend) Encode(in []byte) string {
	// TODO: oh dear -- nomad validVariablePath limits length to 128 chars...
	return base64.RawURLEncoding.EncodeToString(in)
	//return strings.Replace(base64.RawURLEncoding.EncodeToString(in),
	//	"=", "_", -1)
}

func (n *nomadBackend) Decode(in string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(in)
	//return base64.RawURLEncoding.DecodeString(strings.Replace(in,
	//	"_", "=", -1))
	//return []byte(in), nil
}

func (n *nomadBackend) ListAndWatch(ctx context.Context, prefix string, chanSize int) *Watcher {
	w := newWatcher(prefix, chanSize)
	go n.Watch(ctx, w)
	return w
}

func (n *nomadBackend) RegisterLeaseExpiredObserver(prefix string, fn func(key string)) {
	panic("implement me")
}

func (n *nomadBackend) UserEnforcePresence(ctx context.Context, name string, roles []string) error {
	panic("implement me")
}

func (n *nomadBackend) UserEnforceAbsence(ctx context.Context, name string) error {
	panic("implement me")
}
