package MavenProject.Ignite;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.sql.*;
import javax.cache.integration.CacheLoaderException;
import javax.sql.DataSource;

import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.SpringResource;
import org.jetbrains.annotations.Nullable;

public abstract class PersonStore implements CacheStore<Long, Person> {
    @SpringResource(resourceName = "dataSource")
    private DataSource dataSource;
    // This method is called whenever IgniteCache.loadCache() method is called.
    @Override
    public void loadCache(IgniteBiInClosure<Long, Person> clo, @Nullable Object... objects) throws CacheLoaderException {
        System.out.println(">> Loading cache from store...");
        try (Connection conn = dataSource.getConnection()) {
            try (Statement st = conn.createStatement()) {
                try (ResultSet rs = st.executeQuery("select * from PersonDemo")) {
                    while (rs.next()) {
                        Person person = new Person(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getInt(4));
                        clo.apply(person.getId(), person);
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }
    // This method is called whenever IgniteCache.get() method is called.
    public Person load(Long key) throws CacheLoaderException {
        System.out.println(">> Loading person from store...");
        try (Connection conn = dataSource.getConnection()) {
            try (Statement st = conn.createStatement()) {
                ResultSet rs = st.executeQuery("select * from PersonDemo where id = ?");
                return rs.next() ? new Person(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getInt(4)) : null;
            }
        }
        catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }
}