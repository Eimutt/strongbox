package org.carlspring.strongbox.storage.indexing.local;

import org.carlspring.strongbox.artifact.coordinates.MavenArtifactCoordinates;
import org.carlspring.strongbox.domain.ArtifactArchiveListing;
import org.carlspring.strongbox.domain.ArtifactEntry;

import java.io.PrintWriter;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.index.ArtifactContext;
import org.apache.maven.index.ArtifactInfo;
import org.apache.maven.index.creator.JarFileContentsIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Przemyslaw Fusik
 */
public class ArtifactEntryJarFileContentsIndexCreator
        extends JarFileContentsIndexCreator
{

    public static final ArtifactEntryJarFileContentsIndexCreator INSTANCE = new ArtifactEntryJarFileContentsIndexCreator();

    private ArtifactEntryJarFileContentsIndexCreator()
    {
        super();
    }

    private static final Logger logger = LoggerFactory.getLogger(ArtifactEntryJarFileContentsIndexCreator.class);

    private static boolean visited_branches[] = new boolean[15];

    @Override
    public void populateArtifactInfo(ArtifactContext artifactContext)
    {
        ArtifactEntryArtifactContext artifactEntryArtifactContext = (ArtifactEntryArtifactContext) artifactContext;
        ArtifactEntry artifactEntry = artifactEntryArtifactContext.getArtifactEntry();
        ArtifactInfo artifactInfo = artifactEntryArtifactContext.getArtifactInfo();

        final MavenArtifactCoordinates coordinates = (MavenArtifactCoordinates) artifactEntry.getArtifactCoordinates();
        final String extension = coordinates.getExtension();

        if ("jar" .equals(extension) ||
            "war" .equals(extension) ||
            "zip" .equals(extension))
        {
            updateArtifactInfo(artifactInfo, artifactEntry);
        }
    }

    /**
     * @see JarFileContentsIndexCreator#updateArtifactInfo(org.apache.maven.index.ArtifactInfo, java.io.File)
     */
    private void updateArtifactInfo(final ArtifactInfo artifactInfo,
                                    final ArtifactEntry artifactEntry)
    {
        final MavenArtifactCoordinates coordinates = (MavenArtifactCoordinates) artifactEntry.getArtifactCoordinates();

        String strippedPrefix = null;
        if ("war" .equals(coordinates.getExtension()))
        {
            strippedPrefix = "WEB-INF/classes/";
        }

        updateArtifactInfo(artifactInfo, artifactEntry, strippedPrefix);
    }

    /**
     * @see org.apache.maven.index.creator.JarFileContentsIndexCreator#updateArtifactInfo(org.apache.maven.index.ArtifactInfo, java.io.File, java.lang.String)
     */
    private void updateArtifactInfo(final ArtifactInfo artifactInfo,
                                    final ArtifactEntry artifactEntry,
                                    final String strippedPrefix)
    {

        //entering if/else with 2 conditions. Since the condition can be re-stated as two separate if statements,
        //..and both lead to the return statement, the return statement is counted twice. CC = 1+1+1+1 = 4.
        ArtifactArchiveListing artifactArchiveListing = artifactEntry.getArtifactArchiveListing();
        if (artifactArchiveListing == null || CollectionUtils.isEmpty(artifactArchiveListing.getFilenames()))
        {
            visited_branches[0]=true;
            return;
        }else{
            visited_branches[1]=true;
        }

        Set<String> filenames = artifactArchiveListing.getFilenames();

        final StringBuilder sb = new StringBuilder();

        //for loop, CC = 5
        for (final String name : filenames)
        {
            //entering if/else, CC = 6
            visited_branches[2]=true;
            if (name.endsWith(".class"))
            {
                visited_branches[3]=true;
                // original maven indexer skips inner classes too
                final int i = name.indexOf("$");

                //entering if/else, CC = 7
                if (i == -1)
                {
                    //entering if/else, CC = 8
                    visited_branches[4]=true;
                    if (name.charAt(0) != '/')
                    {
                        visited_branches[5]=true;
                        sb.append('/');
                    }else{
                        visited_branches[6]=true;
                    }

                    //entering if / else if / else, CC = 10
                    if (StringUtils.isBlank(strippedPrefix))
                    {
                        visited_branches[7]=true;
                        // class name without ".class"
                        sb.append(name, 0, name.length() - 6).append('\n');
                    }
                    else if (name.startsWith(strippedPrefix)
                             && (name.length() > (strippedPrefix.length() + 6)))
                    {
                        visited_branches[8]=true;
                        // class name without ".class" and stripped prefix
                        sb.append(name, strippedPrefix.length(), name.length() - 6).append('\n');
                    }else{
                        visited_branches[9]=true;
                    }
                }else{
                    visited_branches[10]=true;
                }
            }else{
                visited_branches[11]=true;
            }
        }
        visited_branches[12]=true;

        final String fieldValue = sb.toString().trim();

        logger.debug("Updating ArtifactInfo using artifactEntry [{}] by classNames [{}]",
                     artifactEntry,
                     fieldValue);

        //entering if/else, CC = 11
        if (fieldValue.length() != 0)
        {
            visited_branches[13]=true;
            artifactInfo.setClassNames(fieldValue);
        }
        else
        {
            visited_branches[14]=true;
            artifactInfo.setClassNames(null);
        }
    }

    public static void printResultCoverage(){
        PrintWriter writer = null;
        try{
            writer = new PrintWriter("../../CoverageArtifactEntry.txt", "UTF-8");
            for (int i = 0 ; i < visited_branches.length; i++) {
                writer.println("Branch "+i+" -> "+visited_branches[i]);
            }
        } catch(Exception e){
            System.err.println(e);
        } finally{
            writer.close();
        }
    }
}
