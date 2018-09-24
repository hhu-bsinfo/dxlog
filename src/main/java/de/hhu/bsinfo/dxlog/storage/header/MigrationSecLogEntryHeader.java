/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxlog.storage.header;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxlog.storage.versioncontrol.Version;

/**
 * Extends AbstractLogEntryHeader for a migration log entry header (secondary log)
 * Fields: | Type | NodeID | LocalID | Length  | Timestamp | Epoch | Version | Chaining | Checksum |
 * Length: |  1   |   2    | 1,2,4,6 | 0,1,2,3 |    0,4    |   2   | 0,1,2,4 |   0,2    |    0,4   |
 * Type field contains type, length of LocalID field, length of length field and length of version field
 * Timestamp field has length 0 if timestamps are deactivated in DXRAM configuration, 4 otherwise
 * Chaining field has length 0 for chunks smaller than 1/2 of segment size (4 MB default) and 2 for larger chunks
 * (chaining ID + chain size)
 * Checksum field has length 0 if checksums are deactivated in DXRAM configuration, 4 otherwise
 * Log entry headers are read and written with absolute methods (position is untouched), only!
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.06.2015
 */
final class MigrationSecLogEntryHeader extends AbstractSecLogEntryHeader {

    private static byte ms_nidOffset;
    private static byte ms_lidOffset;

    /**
     * Creates an instance of MigrationSecLogEntryHeader
     */
    MigrationSecLogEntryHeader() {
        ms_nidOffset = LOG_ENTRY_TYP_SIZE;
        ms_lidOffset = (byte) (ms_nidOffset + LOG_ENTRY_NID_SIZE);
    }

    @Override
    public short getNodeID(final ByteBuffer p_buffer, final int p_offset) {
        final int offset = p_offset + ms_nidOffset;

        return p_buffer.getShort(offset);
    }

    @Override
    public long getCID(final ByteBuffer p_buffer, final int p_offset) {
        return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_buffer, p_offset);
    }

    @Override
    public long getCID(final short p_type, final ByteBuffer p_buffer, final int p_offset) {
        return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_type, p_buffer, p_offset);
    }

    @Override
    public boolean isMigrated() {
        return true;
    }

    @Override
    short getNIDOffset() {
        return ms_nidOffset;
    }

    @Override
    short getLIDOffset() {
        return ms_lidOffset;
    }

    @Override
    public void print(final ByteBuffer p_buffer, final int p_offset) {
        final Version version = getVersion(p_buffer, p_offset);

        System.out.println("********************Secondary Log Entry Header (Migration)********************");
        System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
        System.out.println("* Length: " + getLength(p_buffer, p_offset));
        System.out.println("* Version: " + version.getEpoch() + ", " + version.getVersion());
        if (ms_timestampSize > 0) {
            System.out.println("* Timestamp: " + getTimestamp(p_buffer, p_offset));
        }
        if (ChecksumHandler.checksumsEnabled()) {
            System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
        }
        System.out.println("******************************************************************************");
    }

}
