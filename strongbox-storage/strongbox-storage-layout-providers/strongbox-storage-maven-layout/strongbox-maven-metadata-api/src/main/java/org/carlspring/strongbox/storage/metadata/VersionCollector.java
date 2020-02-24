package org.carlspring.strongbox.storage.metadata;

import org.carlspring.strongbox.storage.metadata.maven.io.filters.ArtifactVersionDirectoryFilter;
import org.carlspring.strongbox.storage.metadata.maven.comparators.MetadataVersionComparator;
import org.carlspring.strongbox.storage.metadata.maven.comparators.SnapshotVersionComparator;
import org.carlspring.strongbox.storage.metadata.maven.versions.MetadataVersion;
import org.carlspring.strongbox.storage.metadata.maven.visitors.ArtifactVersionDirectoryVisitor;

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.ArtifactUtils;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.DefaultArtifactHandler;
import org.apache.maven.artifact.repository.metadata.Plugin;
import org.apache.maven.artifact.repository.metadata.SnapshotVersion;
import org.apache.maven.artifact.repository.metadata.Versioning;
import org.apache.maven.index.artifact.Gav;
import org.apache.maven.index.artifact.M2GavCalculator;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author stodorov
 */
public class VersionCollector
{
    public static boolean visited_branches[] = new boolean[21];

    private static final Logger logger = LoggerFactory.getLogger(VersionCollector.class);

    private static final M2GavCalculator M2_GAV_CALCULATOR = new M2GavCalculator();

    public VersionCollectionRequest collectVersions(Path artifactBasePath)
            throws IOException
    {
        VersionCollectionRequest request = new VersionCollectionRequest();
        request.setArtifactBasePath(artifactBasePath);

        List<MetadataVersion> versions = new ArrayList<>();

        List<Path> versionPaths;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(artifactBasePath,
                                                                 new ArtifactVersionDirectoryFilter()))
        {
            //not counting this try towards cyclomatic complexity since the error is thrown upwards outside this class.
            visited_branches[0]=true;
            versionPaths = Lists.newArrayList(ds);
        }

        // Add all versions
        for (Path versionDirectoryPath : versionPaths)
        {
            //we enter for loop: CC = 1
            visited_branches[1]=true;
            try
            {
                //we enter a try/catch block, CC = 2
                visited_branches[2]=true;
                Path pomArtifactPath = getPomPath(artifactBasePath, versionDirectoryPath);

                //entering if/else, CC = 3
                if (pomArtifactPath != null)
                {
                    visited_branches[3] = true;
                    Model pom = getPom(pomArtifactPath);

                    BasicFileAttributes fileAttributes = Files.readAttributes(versionDirectoryPath,
                                                                              BasicFileAttributes.class);

                    //double if statement, CC = 5
                    String version = pom.getVersion() != null ? testHelp(pom.getVersion(),4) :
                                     (pom.getParent() != null ? testHelp(pom.getVersion(),5) : testHelp(null,6));

                    //entering if/else, CC = 6
                    if (version == null)
                    {
                        visited_branches[7]=true;
                        continue;
                    }else{
                        visited_branches[8]=true;
                    }

                    //entering if/else, CC = 7
                    if (ArtifactUtils.isSnapshot(version))
                    {
                        visited_branches[9]=true;
                        version = ArtifactUtils.toSnapshotVersion(version);
                    }else{
                        visited_branches[10]=true;
                    }

                    MetadataVersion metadataVersion = new MetadataVersion();
                    metadataVersion.setVersion(version);
                    metadataVersion.setCreatedDate(fileAttributes.lastModifiedTime());

                    versions.add(metadataVersion);

                    //entering if/else, CC = 8
                    if (artifactIsPlugin(pom))
                    {
                        visited_branches[11]=true;
                        //another if/else, CC = 9
                        String name = pom.getName() != null ? testHelp(pom.getName(),12) : testHelp(pom.getArtifactId(),13);

                        // TODO: SB-339: Get the maven plugin's prefix properly when generating metadata
                        // TODO: This needs to be addressed properly, as it's not correct.
                        // TODO: This can be obtained from the jar's META-INF/maven/plugin.xml and should be read
                        // TODO: either via a ZipInputStream, or using TrueZip.
                        // String prefix = pom.getArtifactId().replace("maven-plugin", "").replace("-plugin$", "");

                        Plugin plugin = new Plugin();
                        plugin.setName(name);
                        plugin.setArtifactId(pom.getArtifactId());
                        plugin.setPrefix(PluginDescriptor.getGoalPrefixFromArtifactId(pom.getArtifactId()));

                        request.addPlugin(plugin);
                    }else{
                        visited_branches[14]=true;
                    }
                }else{
                    visited_branches[15]=true;
                }
            }
            catch (XmlPullParserException | IOException e)
            {
                visited_branches[16]=true;
                logger.error("POM file '{}' appears to be corrupt.", versionDirectoryPath.toAbsolutePath(), e);
            }
        }
        visited_branches[17]=true;


        //entering if/else, CC = 10
        if (!versions.isEmpty())
        {
            visited_branches[18]=true;
            Collections.sort(versions, new MetadataVersionComparator());
        }else{
            visited_branches[19]=true;
        }

        request.setMetadataVersions(versions);
        request.setVersioning(generateVersioning(versions));

        //reached return, CC = 11
        visited_branches[20]=true;
        return request;
    }

    private String testHelp(String test, int branchN){
        visited_branches[branchN] = true;
        return test;
    }

    public static void printResultCoverage(){
        PrintWriter writer = null;
        try{
            writer = new PrintWriter("../../CoverageVersionCollector.txt", "UTF-8");
            for (int i = 0 ; i < visited_branches.length; i++) {
                writer.println("Branch "+i+" -> "+visited_branches[i]);
            }
        } catch(Exception e){
            System.err.println(e);
        } finally{
            writer.close();
        }
    }


    private Path getPomPath(Path artifactBasePath,
                            Path versionDirectoryPath)
    {
        String version = versionDirectoryPath.getFileName().toString();
        if (!ArtifactUtils.isSnapshot(version))
        {
            return Paths.get(versionDirectoryPath.toAbsolutePath().toString(),
                             artifactBasePath.getFileName().toString() + "-" +
                             versionDirectoryPath.getFileName() + ".pom");
        }
        else
        {
            // Attempt to get the latest available POM
            List<String> filePaths = Arrays.asList(versionDirectoryPath.toFile()
                                                                       .list((dir, name) -> name.endsWith(".pom")));

            if (filePaths != null && !filePaths.isEmpty())
            {
                Collections.sort(filePaths);
                return Paths.get(versionDirectoryPath.toAbsolutePath().toString(),
                                 filePaths.get(filePaths.size() - 1));
            }
            else
            {
                return null;
            }
        }
    }

    /**
     * Get snapshot versioning information for every released snapshot
     *
     * @param artifactVersionPath
     * @throws IOException
     */
    public List<SnapshotVersion> collectTimestampedSnapshotVersions(Path artifactVersionPath)
            throws IOException
    {
        List<SnapshotVersion> snapshotVersions = new ArrayList<>();

        ArtifactVersionDirectoryVisitor artifactVersionDirectoryVisitor = new ArtifactVersionDirectoryVisitor();

        Files.walkFileTree(artifactVersionPath, artifactVersionDirectoryVisitor);

        for (Path filePath : artifactVersionDirectoryVisitor.getMatchingPaths())
        {
            String unixBasedFilePath = FilenameUtils.separatorsToUnix(filePath.toString());
            Gav gav = M2_GAV_CALCULATOR.pathToGav(unixBasedFilePath);

            Artifact artifact = new DefaultArtifact(gav.getGroupId(),
                                                    gav.getArtifactId(),
                                                    gav.getVersion(),
                                                    null,
                                                    gav.getExtension(),
                                                    gav.getClassifier(),
                                                    new DefaultArtifactHandler(gav.getExtension()));

            String name = filePath.toFile().getName();

            SnapshotVersion snapshotVersion = MetadataHelper.createSnapshotVersion(artifact,
                                                                                   FilenameUtils.getExtension(name));

            snapshotVersions.add(snapshotVersion);
        }

        if (!snapshotVersions.isEmpty())
        {
            Collections.sort(snapshotVersions, new SnapshotVersionComparator());
        }

        return snapshotVersions;
    }

    public Versioning generateVersioning(List<MetadataVersion> versions)
    {
        Versioning versioning = new Versioning();

        if (!versions.isEmpty())
        {
            // Sort versions naturally (1.1 < 1.2 < 1.3 ...)
            Collections.sort(versions, new MetadataVersionComparator());
            for (MetadataVersion version : versions)
            {
                versioning.addVersion(version.getVersion());
            }

            // Sort versions naturally but consider creation date as well so that
            // 1.1 < 1.2 < 1.4 < 1.3 (1.3 is considered latest release because it was changed recently)
            // TODO: Sort this out as part of SB-333
            //Collections.sort(versions);
        }

        return versioning;
    }

    public Versioning generateSnapshotVersions(List<SnapshotVersion> snapshotVersionList)
    {
        Versioning versioning = new Versioning();

        if (!snapshotVersionList.isEmpty())
        {
            versioning.setSnapshotVersions(snapshotVersionList);
        }

        return versioning;
    }

    private boolean artifactIsPlugin(Model model)
    {
        return model.getPackaging().equals("maven-plugin");
    }

    private Model getPom(Path filePath)
            throws IOException, XmlPullParserException
    {
        try (Reader rr = new FileReader(filePath.toFile()))
        {
            MavenXpp3Reader reader = new MavenXpp3Reader();
            return reader.read(rr);
        }

    }

}
