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

package de.hhu.bsinfo.dxlog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxlog.storage.BackupRangeCatalog;
import de.hhu.bsinfo.dxlog.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxlog.storage.Scheduler;
import de.hhu.bsinfo.dxlog.storage.diskaccess.HarddriveAccessMode;
import de.hhu.bsinfo.dxlog.storage.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxlog.storage.header.ChecksumHandler;
import de.hhu.bsinfo.dxlog.storage.logs.Log;
import de.hhu.bsinfo.dxlog.storage.logs.LogHandler;
import de.hhu.bsinfo.dxlog.storage.recovery.FileRecoveryHandler;
import de.hhu.bsinfo.dxlog.storage.recovery.LogRecoveryHandler;
import de.hhu.bsinfo.dxlog.storage.recovery.RecoveryMetadata;
import de.hhu.bsinfo.dxlog.storage.versioncontrol.VersionHandler;
import de.hhu.bsinfo.dxlog.storage.writebuffer.BufferPool;
import de.hhu.bsinfo.dxlog.storage.writebuffer.WriteBufferHandler;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.operations.Recovery;
import de.hhu.bsinfo.dxnet.core.MessageHeader;
import de.hhu.bsinfo.dxutils.jni.JNIFileRaw;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;

/**
 * DXLog: Fast object logging, reorganization and recovery.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 24.09.2018
 */
public final class DXLog {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXLog.class.getSimpleName());

    private static final TimePool SOP_LOG_BATCH = new TimePool(DXLog.class, "LogBatch");
    private static final TimePool SOP_PUT_ENTRY_AND_HEADER = new TimePool(DXLog.class, "PutEntryAndHeader");

    private static final String LIB_CRC_GEN = "libJNINativeCRCGenerator.so";
    private static final String LIB_FILE_RAW = "libJNIFileRaw.so";
    private static final String LIB_FILE_ODIRECT = "libJNIFileDirect.so";

    public static final boolean TWO_LEVEL_LOGGING_ACTIVATED = true;

    static {
        StatisticsManager.get().registerOperation(DXLog.class, SOP_LOG_BATCH);
        StatisticsManager.get().registerOperation(DXLog.class, SOP_PUT_ENTRY_AND_HEADER);
    }

    private Recovery m_dxmemRecoveryOp;

    private LogHandler m_logHandler;
    private VersionHandler m_versionHandler;
    private WriteBufferHandler m_writeBufferHandler;
    private LogRecoveryHandler m_logRecoveryHandler;
    private BackupRangeCatalog m_backupRangeCatalog;

    private DXLogConfig m_config;

    private short m_nodeID;
    private long m_secondaryLogSize;
    private String m_backupDirectory;
    private HarddriveAccessMode m_mode;

    private long m_initTime;

    /**
     * Creates an instance of DXLog.
     *
     * @param p_config
     *         the dxlog configuration
     * @param p_pathJniDirectory
     *         Path to directory containing compiled native libraries (JNI)
     * @param p_backupDir
     *         the directory to store logs in
     * @param p_backupRangeSize
     *         the backup range size
     * @param p_dxmemRecoveryOp
     *         the DXMem Recovery operation to store recovered chunk in memory management
     */
    public DXLog(final DXLogConfig p_config, final String p_pathJniDirectory, final short p_nodeID,
            final String p_backupDir, final int p_backupRangeSize, final Recovery p_dxmemRecoveryOp) {
        if (p_config.verify(p_backupRangeSize)) {
            m_config = p_config;

            applyConfiguration(p_config);

            m_nodeID = p_nodeID;
            m_backupDirectory = p_backupDir;
            m_secondaryLogSize = p_backupRangeSize * 2;

            loadJNIModules(p_pathJniDirectory);

            purgeLogDirectory(m_backupDirectory);

            createHandlers();

            m_dxmemRecoveryOp = p_dxmemRecoveryOp;
        } else {
            LOGGER.error("Configuration invalid.");
        }
    }

    private void loadJNIModules(final String p_pathJNIDir) {
        File dir = new File(p_pathJNIDir);

        // Load jni modules
        if (!dir.exists()) {
            throw new RuntimeException("Directory for native libraries (JNI) does not exist: " + dir.getAbsolutePath());
        }

        System.load(dir.getAbsolutePath() + '/' + LIB_CRC_GEN);

        if (m_mode == HarddriveAccessMode.ODIRECT) {
            System.load(dir.getAbsolutePath() + '/' + LIB_FILE_ODIRECT);
        } else if (m_mode == HarddriveAccessMode.RAW_DEVICE) {
            System.load(dir.getAbsolutePath() + '/' + LIB_FILE_RAW);

            if (JNIFileRaw.prepareRawDevice(m_config.getRawDevicePath(), 0) == -1) {
                LOGGER.debug("\n     * Steps to prepare a raw device:\n" + "     * 1) Use an empty partition\n" +
                        "     * 2) If executed in nspawn container: add \"--capability=CAP_SYS_MODULE " +
                        "--bind-ro=/lib/modules\" to systemd-nspawn command in boot script\n" +
                        "     * 3) Get root access\n" + "     * 4) mkdir /dev/raw\n" + "     * 5) cd /dev/raw/\n" +
                        "     * 6) mknod raw1 c 162 1\n" + "     * 7) modprobe raw\n" +
                        "     * 8) If /dev/raw/rawctl was not created: mknod /dev/raw/rawctl c 162 0\n" +
                        "     * 9) raw /dev/raw/raw1 /dev/*empty partition*\n" +
                        "     * 10) Execute DXRAM as root user (sudo -P for nfs)");
                throw new RuntimeException("Raw device could not be prepared!");
            }
        }
    }

    /**
     * Apply configuration for static attributes. Do NOT change the order!
     *
     * @param p_config
     *         the configuration
     */
    private void applyConfiguration(final DXLogConfig p_config) {
        m_mode = HarddriveAccessMode.convert(p_config.getHarddriveAccess());

        if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
            DirectByteBufferWrapper.useNativeBuffers(false);
            ChecksumHandler.useNativeBuffers(false);
        } else {
            DirectByteBufferWrapper.useNativeBuffers(true);
            ChecksumHandler.useNativeBuffers(true);
        }
        DirectByteBufferWrapper.setPageSize((int) p_config.getFlashPageSize().getBytes());
        // Set the segment size. Needed for log entry header to split large chunks (must be called before the first log
        // entry header is created)
        AbstractLogEntryHeader.setSegmentSize((int) p_config.getLogSegmentSize().getBytes());
        // Set the log entry header tsp size (must be called before the first log entry header is created)
        AbstractLogEntryHeader.setTimestampSize(p_config.isUseTimestamps());
        // Set the log entry header crc size (must be called before the first log entry header is created)
        ChecksumHandler.setCRCSize(p_config.isUseChecksums());
        // Set the hard drive access mode (must be called before the first log is created)
        Log.setAccessMode(m_mode);

        m_initTime = System.currentTimeMillis();
    }

    /**
     * Purge all logs from directory.
     *
     * @param p_path
     *         the directory
     */
    private static void purgeLogDirectory(final String p_path) {
        File dir = new File(p_path);
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.isDirectory()) {
                    if (!file.delete()) {
                        // Ignore. Will be overwritten anyways
                    }
                }
            }
        }
    }

    /**
     * Create all handlers and the backup range catalog.
     */
    private void createHandlers() {
        m_backupRangeCatalog = new BackupRangeCatalog();

        Scheduler scheduler = new Scheduler();
        BufferPool bufferPool = new BufferPool((int) m_config.getLogSegmentSize().getBytes());
        m_versionHandler = new VersionHandler(scheduler, m_backupRangeCatalog, m_secondaryLogSize);
        m_logHandler = new LogHandler(m_versionHandler, scheduler, m_backupRangeCatalog, bufferPool,
                m_config.getPrimaryLogSize().getBytes(), m_secondaryLogSize,
                (int) m_config.getSecondaryLogBufferSize().getBytes(), (int) m_config.getLogSegmentSize().getBytes(),
                (int) m_config.getFlashPageSize().getBytes(), m_config.isUseChecksums(),
                m_config.getUtilizationActivateReorganization(), m_config.isUseTimestamps(),
                m_config.getColdDataThresholdInSec(), m_backupDirectory, m_nodeID);
        m_writeBufferHandler = new WriteBufferHandler(m_logHandler, m_versionHandler, scheduler, bufferPool,
                (int) m_config.getWriteBufferSize().getBytes(), (int) m_config.getSecondaryLogBufferSize().getBytes(),
                (int) m_config.getFlashPageSize().getBytes(), m_config.isUseChecksums(), m_config.isUseTimestamps(),
                m_initTime);
        scheduler.set(m_writeBufferHandler, m_logHandler);

        m_logRecoveryHandler =
                new LogRecoveryHandler(m_versionHandler, scheduler, m_backupRangeCatalog, m_secondaryLogSize,
                        (int) m_config.getLogSegmentSize().getBytes(), m_config.isUseChecksums());
    }

    /**
     * Closes DXLog.
     *
     * @return true if successful
     */
    public boolean close() {
        m_writeBufferHandler.close();
        m_logHandler.close();
        m_versionHandler.close();
        m_logRecoveryHandler.close();

        m_backupRangeCatalog.closeLogsAndBuffers();
        m_backupRangeCatalog = null;

        return true;
    }

    /**
     * This is a special receiver message. To avoid creating and deserializing the message,
     * the message header is passed here directly (if complete, split messages are handled normally).
     *
     * @param p_messageHeader
     *         the message header (the payload is yet to be deserialized)
     */
    public void logChunks(final MessageHeader p_messageHeader) {
        m_writeBufferHandler.postData(p_messageHeader);
    }

    /**
     * Logs a buffer with Chunks on SSD
     *
     * @param p_owner
     *         the Chunks' owner
     * @param p_rangeID
     *         the RangeID
     * @param p_numberOfDataStructures
     *         the number of data structures stored in p_buffer
     * @param p_buffer
     *         the Chunk buffer
     */
    public void logChunks(final short p_owner, final short p_rangeID, final int p_numberOfDataStructures,
            final ByteBuffer p_buffer) {
        m_writeBufferHandler.postData(p_owner, p_rangeID, p_numberOfDataStructures, p_buffer);
    }

    /**
     * Removes Chunks from log
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_owner
     *         the Chunks' owner
     * @param p_chunkIDs
     *         the ChunkIDs of all to be deleted chunks
     */
    public void removeChunks(final short p_rangeID, final short p_owner, final long[] p_chunkIDs) {
        m_versionHandler.invalidateChunks(p_chunkIDs, p_owner, p_rangeID);
    }

    /**
     * Initializes a new backup range
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_owner
     *         the Chunks' owner
     * @return whether the operation was successful or not
     */
    public boolean initBackupRange(final short p_rangeID, final short p_owner) {
        return m_logHandler.createBackupRange(p_rangeID, p_owner, m_secondaryLogSize,
                (int) m_config.getLogSegmentSize().getBytes(), (int) m_config.getSecondaryLogBufferSize().getBytes(),
                (int) m_config.getFlashPageSize().getBytes(), m_config.getUtilizationPromptReorganization(),
                m_config.isUseChecksums(), m_config.isUseTimestamps(), m_initTime, m_backupDirectory);
    }

    /**
     * Initializes a backup range after recovery (creating a new one or transferring the old)
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_owner
     *         the Chunks' owner
     * @return whether the backup range is initialized or not
     */
    public boolean initRecoveredBackupRange(final short p_rangeID, final short p_owner, final short p_originalRangeID,
            final short p_originalOwner, final boolean p_isNewBackupRange) {
        return m_logHandler
                .createRecoveredBackupRange(p_rangeID, p_owner, p_originalRangeID, p_originalOwner, p_isNewBackupRange,
                        m_secondaryLogSize, (int) m_config.getLogSegmentSize().getBytes(),
                        (int) m_config.getSecondaryLogBufferSize().getBytes(),
                        (int) m_config.getFlashPageSize().getBytes(), m_config.getUtilizationPromptReorganization(),
                        m_config.isUseChecksums(), m_config.isUseTimestamps(), m_initTime, m_backupDirectory);
    }

    /**
     * Removes the logs and buffers from given backup range.
     *
     * @param p_owner
     *         the owner of the backup range
     * @param p_rangeID
     *         the RangeID
     */
    public void removeBackupRange(final short p_owner, final short p_rangeID) {
        m_logHandler.removeBackupRange(p_owner, p_rangeID);
    }

    /**
     * Recovers all Chunks of given backup range
     *
     * @param p_owner
     *         the NodeID of the node whose Chunks have to be restored
     * @param p_rangeID
     *         the RangeID
     * @return the recovery metadata
     */
    public RecoveryMetadata recoverBackupRange(final short p_owner, final short p_rangeID) {
        return m_logRecoveryHandler.recoverBackupRange(p_owner, p_rangeID, m_dxmemRecoveryOp);
    }

    /**
     * Recovers all Chunks of given backup range
     *
     * @param p_fileName
     *         the file name
     * @param p_path
     *         the path of the folder the file is in
     * @return the recovered Chunks
     */
    public AbstractChunk[] recoverBackupRangeFromFile(final String p_fileName, final String p_path) {
        AbstractChunk[] ret = null;

        try {
            ret = FileRecoveryHandler.recoverFromFile(p_fileName, p_path, m_config.isUseChecksums(), m_secondaryLogSize,
                    (int) m_config.getLogSegmentSize().getBytes());
        } catch (final IOException e) {

            LOGGER.error("Could not recover from file %s: %s", p_path, e);

        }

        return ret;
    }

    /**
     * Returns the current utilization of primary log and all secondary logs
     *
     * @return the current utilization
     */
    public String getCurrentUtilization() {
        return m_logHandler.getCurrentUtilization();
    }

}
