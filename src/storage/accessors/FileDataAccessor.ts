import type { Stats } from 'fs';
import { createWriteStream, createReadStream, promises as fsPromises } from 'fs';
import { posix } from 'path';
import type { Readable } from 'stream';
import { DataFactory } from 'n3';
import type { NamedNode, Quad } from 'rdf-js';
import type { Representation } from '../../ldp/representation/Representation';
import { RepresentationMetadata } from '../../ldp/representation/RepresentationMetadata';
import type { ResourceIdentifier } from '../../ldp/representation/ResourceIdentifier';
import { ConflictHttpError } from '../../util/errors/ConflictHttpError';
import { NotFoundHttpError } from '../../util/errors/NotFoundHttpError';
import { isSystemError } from '../../util/errors/SystemError';
import { UnsupportedMediaTypeHttpError } from '../../util/errors/UnsupportedMediaTypeHttpError';
import type { MetadataController } from '../../util/MetadataController';
import { CONTENT_TYPE, DCTERMS, POSIX, RDF, XSD } from '../../util/UriConstants';
import { toNamedNode, toTypedLiteral } from '../../util/UriUtil';
import { ensureTrailingSlash } from '../../util/Util';
import type { ExtensionBasedMapper } from '../ExtensionBasedMapper';
import { NormalizedDataAccessor } from './NormalizedDataAccessor';

const { join: joinPath } = posix;

/**
 * DataAccessor that uses the file system to store data resources as files and containers as folders.
 */
export class FileDataAccessor extends NormalizedDataAccessor {
  private readonly resourceMapper: ExtensionBasedMapper;
  private readonly metadataController: MetadataController;

  public constructor(resourceMapper: ExtensionBasedMapper, metadataController: MetadataController) {
    super();
    this.resourceMapper = resourceMapper;
    this.metadataController = metadataController;
  }

  /**
   * Only binary data can be directly stored as files so will error on non-binary data.
   */
  public async canHandle(representation: Representation): Promise<void> {
    if (!representation.binary) {
      throw new UnsupportedMediaTypeHttpError('Only binary data is supported.');
    }
  }

  /**
   * Will return data stream directly to the file corresponding to the resource.
   * Will throw NotFoundHttpError if the input is a container.
   */
  public async getData(identifier: ResourceIdentifier): Promise<Readable> {
    const path = this.resourceMapper.mapUrlToFilePath(identifier);
    const stats = await this.getStats(path);

    if (stats.isFile()) {
      return createReadStream(path);
    }

    throw new NotFoundHttpError();
  }

  /**
   * Will return corresponding metadata by reading the metadata file (if it exists)
   * and adding file system specific metadata elements.
   */
  public async getMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
    const path = this.resourceMapper.mapUrlToFilePath(identifier);
    const stats = await this.getStats(path);
    if (!identifier.path.endsWith('/') && stats.isFile()) {
      return this.getFileMetadata(identifier, path, stats);
    }
    if (identifier.path.endsWith('/') && stats.isDirectory()) {
      return this.getDirectoryMetadata(identifier, path, stats);
    }
    throw new NotFoundHttpError();
  }

  /**
   * Writes the given data as a file (and potential metadata as additional file).
   * The metadata file will be written first and will be deleted if something goes wrong writing the actual data.
   */
  public async writeDataResource(identifier: ResourceIdentifier, data: Readable, metadata?: RepresentationMetadata):
  Promise<void> {
    const path = this.resourceMapper.mapUrlToFilePath(identifier);
    if (this.isMetadataPath(path)) {
      throw new ConflictHttpError('Not allowed to create files with the metadata extension.');
    }
    if (metadata) {
      // These are stored by file system conventions
      // Note that we currently don't remove content type since it is not stored correctly yet
      metadata.removeAll(RDF.type);
      const quads = metadata.quads();
      if (quads.length > 0) {
        const serializedMetadata = this.metadataController.serializeQuads(quads);
        await this.writeDataFile(this.getMetadataPath(path), serializedMetadata);
      }
    }
    try {
      await this.writeDataFile(path, data);
    } catch (error: unknown) {
      // Delete the metadata if there was an error writing the file
      if (metadata) {
        await fsPromises.unlink(this.getMetadataPath(path));
      }
      throw error;
    }
  }

  /**
   * Creates corresponding folder if necessary and writes metadata to metadata file if necessary.
   */
  public async writeContainer(identifier: ResourceIdentifier, metadata?: RepresentationMetadata): Promise<void> {
    const path = this.resourceMapper.mapUrlToFilePath(identifier);
    try {
      await fsPromises.mkdir(path);
    } catch (error: unknown) {
      // Don't throw if directory already exists
      if (!isSystemError(error) || error.code !== 'EEXIST') {
        throw error;
      }
    }
    if (metadata) {
      // These are stored by file system conventions
      metadata.removeAll(RDF.type);
      const quads = metadata.quads();
      if (quads.length > 0) {
        await this.writeDataFile(this.getMetadataPath(path), this.metadataController.serializeQuads(quads));
      }
    }
  }

  /**
   * @throws Not supported.
   */
  public async modifyResource(): Promise<void> {
    throw new Error('Not supported.');
  }

  /**
   * Removes the corresponding file/folder (and metadata file).
   */
  public async deleteResource(identifier: ResourceIdentifier): Promise<void> {
    const path = this.resourceMapper.mapUrlToFilePath(identifier);
    const stats = await this.getStats(path);

    try {
      await fsPromises.unlink(this.getMetadataPath(path));
    } catch (error: unknown) {
      // Ignore if it doesn't exist
      if (!isSystemError(error) || error.code !== 'ENOENT') {
        throw error;
      }
    }

    if (!identifier.path.endsWith('/') && stats.isFile()) {
      await fsPromises.unlink(path);
    } else if (identifier.path.endsWith('/') && stats.isDirectory()) {
      await fsPromises.rmdir(path);
    } else {
      throw new NotFoundHttpError();
    }
  }

  /**
   * Gets the Stats object corresponding to the given file path.
   * @param path - File path to get info from.
   *
   * @throws NotFoundHttpError
   * If the file/folder doesn't exist.
   */
  private async getStats(path: string): Promise<Stats> {
    try {
      return await fsPromises.lstat(path);
    } catch (error: unknown) {
      if (isSystemError(error) && error.code === 'ENOENT') {
        throw new NotFoundHttpError();
      }
      throw error;
    }
  }

  /**
   * Generates file path that corresponds to the metadata file of the given file path.
   */
  private getMetadataPath(path: string): string {
    return `${path}.meta`;
  }

  /**
   * Checks if the given file path is a metadata path.
   */
  private isMetadataPath(path: string): boolean {
    return path.endsWith('.meta');
  }

  /**
   * Reads and generates all metadata relevant for the given file,
   * ingesting it into a RepresentationMetadata object.
   *
   * @param identifier - Identifier of the resource.
   * @param path - File path of the corresponding file.
   * @param stats - Stats objects of the corresponding file.
   */
  private async getFileMetadata(identifier: ResourceIdentifier, path: string, stats: Stats):
  Promise<RepresentationMetadata> {
    const contentType = this.resourceMapper.getContentTypeFromExtension(path);

    return (await this.getBaseMetadata(identifier, path, stats, false))
      .set(CONTENT_TYPE, contentType);
  }

  /**
   * Reads and generates all metadata relevant for the given directory,
   * ingesting it into a RepresentationMetadata object.
   *
   * @param identifier - Identifier of the resource.
   * @param path - File path of the corresponding directory.
   * @param stats - Stats objects of the corresponding directory.
   */
  private async getDirectoryMetadata(identifier: ResourceIdentifier, path: string, stats: Stats):
  Promise<RepresentationMetadata> {
    return (await this.getBaseMetadata(identifier, path, stats, true))
      .addQuads(await this.getChildMetadataQuads(identifier, path));
  }

  /**
   * Generates metadata relevant for any resources stored by this accessor.
   * @param identifier - Identifier of the resource.
   * @param path - File path of the corresponding directory.
   * @param stats - Stats objects of the corresponding directory.
   * @param isContainer - If the path points to a container (directory) or not.
   */
  private async getBaseMetadata(identifier: ResourceIdentifier, path: string, stats: Stats, isContainer: boolean):
  Promise<RepresentationMetadata> {
    const metadata = new RepresentationMetadata(identifier.path)
      .addQuads(await this.getRawMetadata(path));
    metadata.addQuads(this.metadataController.generateResourceQuads(metadata.identifier as NamedNode, isContainer));
    metadata.addQuads(this.generatePosixQuads(metadata.identifier as NamedNode, stats));
    return metadata;
  }

  /**
   * Reads the metadata from the corresponding metadata file.
   * Returns an empty array if there is no metadata file.
   *
   * @param path - File path of the resource (not the metadata!).
   */
  private async getRawMetadata(path: string): Promise<Quad[]> {
    try {
      // Check if the metadata file exists first
      await fsPromises.lstat(this.getMetadataPath(path));

      const readMetadataStream = createReadStream(this.getMetadataPath(path));
      return await this.metadataController.parseQuads(readMetadataStream);
    } catch (error: unknown) {
      // Metadata file doesn't exist so lets keep `rawMetaData` an empty array.
      if (!isSystemError(error) || error.code !== 'ENOENT') {
        throw error;
      }
      return [];
    }
  }

  /**
   * Generate all containment related triples for a container.
   * These include the actual containment triples and specific triples for every child resource.
   *
   * @param identifier - Identifier of the container.
   * @param path - File path to the corresponding folder.
   */
  private async getChildMetadataQuads(identifier: ResourceIdentifier, path: string): Promise<Quad[]> {
    const quads: Quad[] = [];
    const childURIs: string[] = [];
    const files = await fsPromises.readdir(path);
    for (const childName of files) {
      // Hide metadata files from containment triples
      if (this.isMetadataPath(childName)) {
        continue;
      }

      const childStats = await fsPromises.lstat(joinPath(path, childName));
      if (!childStats.isFile() && !childStats.isDirectory()) {
        continue;
      }
      let childURI = this.resourceMapper.mapFilePathToUrl(joinPath(path, childName));
      if (childStats.isDirectory()) {
        childURI = ensureTrailingSlash(childURI);
      }

      const subject = DataFactory.namedNode(childURI);
      quads.push(...this.metadataController.generateResourceQuads(subject, childStats.isDirectory()));
      quads.push(...this.generatePosixQuads(subject, childStats));
      childURIs.push(childURI);
    }

    const containsQuads = this.metadataController.generateContainerContainsResourceQuads(
      DataFactory.namedNode(identifier.path), childURIs,
    );

    return quads.concat(containsQuads);
  }

  /**
   * Helper function to add file system related metadata.
   * @param subject - Subject for the new quads.
   * @param stats - Stats of the file/directory corresponding to the resource.
   */
  private generatePosixQuads(subject: NamedNode, stats: Stats): Quad[] {
    const quads: Quad[] = [];
    quads.push(DataFactory.quad(subject, toNamedNode(POSIX.size), toTypedLiteral(stats.size, XSD.integer)));
    quads.push(DataFactory.quad(subject,
      toNamedNode(DCTERMS.modified),
      toTypedLiteral(stats.mtime.toISOString(), XSD.dateTime)));
    quads.push(DataFactory.quad(subject,
      toNamedNode(POSIX.mtime),
      toTypedLiteral(Math.floor(stats.mtime.getTime() / 1000), XSD.integer)));
    return quads;
  }

  /**
   * Helper function without extra validation checking to create a data file.
   * @param path - The filepath of the file to be created.
   * @param data - The data to be put in the file.
   */
  private async writeDataFile(path: string, data: Readable): Promise<void> {
    return new Promise((resolve, reject): any => {
      const writeStream = createWriteStream(path);
      data.pipe(writeStream);
      data.on('error', reject);

      writeStream.on('error', reject);
      writeStream.on('finish', resolve);
    });
  }
}
