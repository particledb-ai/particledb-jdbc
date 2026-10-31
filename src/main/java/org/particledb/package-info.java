/**
 * ParticleDB native JDBC driver ({@code jdbc:particledb://host:7173/database}).
 *
 * <p>Requires Java 11 or later and no JVM flags on any JDK. Results arrive as
 * Arrow IPC and are decoded by the driver's own pure-Java reader
 * ({@link org.particledb.ArrowIpcReader}) for every column type; Apache Arrow
 * Java is an optional dependency, used only when the connection property
 * {@code resultDecoder=arrow} asks for it ({@link org.particledb.ArrowDecoder.ResultDecoder})
 * and by the {@code PdbConnection.bulkLoad} overloads that take the caller's
 * own Arrow vectors. Only those paths need Arrow Java on the classpath and, on
 * JDK 16 and later, {@link org.particledb.ArrowDecoder#ADD_OPENS_FLAG}.
 */
package org.particledb;
