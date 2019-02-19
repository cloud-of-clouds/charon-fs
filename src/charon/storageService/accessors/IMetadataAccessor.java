/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package charon.storageService.accessors;

import charon.general.NSAccessInfo;
import depsky.client.messages.metadata.ExternalMetadata;

/**
 *
 * @author ricardo
 */
public interface IMetadataAccessor {
    
    public byte[] readNS(String idPath, NSAccessInfo accInfo);
    
    public boolean writeNS(String idPath, byte[] value);

    public boolean lock(String ns, int time);
    
}
