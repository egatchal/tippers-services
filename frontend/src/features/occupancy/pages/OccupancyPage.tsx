import { useState } from 'react';
import { Building2 } from 'lucide-react';
import type { Space, OccupancyDataset } from '../types/occupancy';
import SpaceTree from '../components/SpaceTree';
import CreateDatasetForm from '../components/CreateDatasetForm';
import OccupancyDatasetList from '../components/OccupancyDatasetList';
import OccupancyResultsPanel from '../components/OccupancyResultsPanel';

export default function OccupancyPage() {
  const [selectedSpace, setSelectedSpace] = useState<Space | null>(null);
  const [selectedDataset, setSelectedDataset] = useState<OccupancyDataset | null>(null);

  return (
    <div className="h-full flex">
      {/* Left panel — Space tree */}
      <div className="w-72 shrink-0 border-r border-gray-200 bg-white flex flex-col">
        <div className="px-4 py-3 border-b border-gray-100">
          <h2 className="text-sm font-semibold text-gray-800">Spaces</h2>
          <p className="text-xs text-gray-400">Browse the building hierarchy</p>
        </div>
        <div className="flex-1 overflow-y-auto">
          <SpaceTree
            selectedSpaceId={selectedSpace?.space_id ?? null}
            onSelect={(space) => {
              setSelectedSpace(space);
              setSelectedDataset(null);
            }}
          />
        </div>
      </div>

      {/* Right panel — form + datasets + results */}
      <div className="flex-1 overflow-y-auto p-6">
        {!selectedSpace ? (
          <div className="flex flex-col items-center justify-center h-full text-center">
            <Building2 className="w-12 h-12 text-gray-200 mb-3" />
            <h3 className="text-sm font-medium text-gray-500">Select a space</h3>
            <p className="text-xs text-gray-400 mt-1">
              Expand a building in the tree and click a space to get started.
            </p>
          </div>
        ) : (
          <div className="max-w-3xl mx-auto space-y-6">
            {/* Selected space header */}
            <h2 className="text-lg font-semibold text-gray-900">
              {selectedSpace.space_name ?? `Space ${selectedSpace.space_id}`}
            </h2>

            {/* Create dataset form */}
            <CreateDatasetForm
              key={selectedSpace.space_id}
              space={selectedSpace}
              onCreated={(dataset) => setSelectedDataset(dataset)}
            />

            {/* Dataset list */}
            <div>
              <h3 className="text-sm font-semibold text-gray-700 mb-2">Datasets</h3>
              <OccupancyDatasetList
                rootSpaceId={selectedSpace.space_id}
                selectedDatasetId={selectedDataset?.dataset_id ?? null}
                onSelectDataset={setSelectedDataset}
              />
            </div>

            {/* Results */}
            {selectedDataset && (
              <div>
                <h3 className="text-sm font-semibold text-gray-700 mb-2">
                  Results: {selectedDataset.name}
                  {selectedSpace.space_id !== selectedDataset.root_space_id && (
                    <span className="text-xs font-normal text-gray-400 ml-2">
                      (viewing {selectedSpace.space_name ?? `Space ${selectedSpace.space_id}`})
                    </span>
                  )}
                </h3>
                <OccupancyResultsPanel
                  key={`${selectedDataset.dataset_id}-${selectedSpace.space_id}`}
                  dataset={selectedDataset}
                  spaceId={selectedSpace.space_id}
                />
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
