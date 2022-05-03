package v1alpha1

func (tp *TLSPolicy) IsSecureClient() bool {
	if tp == nil || tp.Static == nil {
		return false
	}
	return len(tp.Static.OperatorSecret) != 0
}

func (tp *TLSPolicy) IsSecurePeer() bool {
	if tp == nil || tp.Static == nil || tp.Static.Member == nil {
		return false
	}
	return len(tp.Static.Member.PeerSecret) != 0
}

func (c *EtcdCluster) IsPodPVEnabled() bool {
	if podPolicy := c.Spec.Pod; podPolicy != nil {
		return podPolicy.PersistentVolumeClaimSpec != nil
	}
	return false
}
